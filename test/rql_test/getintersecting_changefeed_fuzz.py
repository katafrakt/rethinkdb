#!/usr/bin/env python
import datetime
import os
import random
import re
import sys
import tempfile
import time
import traceback
import unittest
import functools
import socket
from tornado import gen, ioloop
from tornado.concurrent import Future

sys.path.insert(0, os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                os.pardir, "common"))
import utils

r = utils.import_python_driver()
r.set_loop_type("tornado")

@gen.coroutine
def open_connection():
    if len(sys.argv) > 1:
        port = int(sys.argv[0])
    else:
        port = int(os.getenv('RDB_DRIVER_PORT'))

    conn = yield r.connect(port=port)
    raise gen.Return(conn)

@gen.coroutine
def random_geo(conn):
    def random_point():
        return r.point(random.uniform(-180, 180), random.uniform(-90, 90))

    class_random = random.random()
    if class_random < 0.5:
        # points need no verification
        raise gen.Return(random_point())
    elif class_random < 0.7:
        # lines need no verification
        args = [random_point() for i in range(random.randint(2, 8))]
        raise gen.Return(r.line(r.args(args)))
    else:
        # polygons must obey certain rules, such as not being self-intersecting. we use
        # the server to verify the validity of our (occasionally invalid) polygons.

        # because circles are implemented as polygons and are easier to reason about,
        # we use them.

        while True:
            shell_lat = random.uniform(-180, 180)
            shell_lon = random.uniform(-90, 90)
            shell_radius = random.uniform(0, 100000) # 100km ought to be enough for anyone

            poly = r.circle(r.point(shell_lat, shell_lon), shell_radius)
            try:
                yield poly.run(conn)
            except r.ReqlRuntimeError:
                continue

            if random.random() < 0.5:
                # try to add a hole. occasionally, these holes will be outside of the
                # shell, so we try repeatedly.

                # 112353 meters ~= 1 degree at equator, the largest it could be
                radius_in_deg = shell_radius / 112353
                while True:
                    hole_lat = shell_lat + random.uniform(-radius_in_deg, radius_in_deg)
                    hole_lon = shell_lon + random.uniform(-radius_in_deg, radius_in_deg)
                    hole_radius = random.uniform(0, shell_radius)

                    hole = r.circle(r.point(hole_lat, hole_lon), hole_radius)
                    try:
                        yield poly.polygon_sub(hole).run(conn)
                    except r.ReqlRuntimeError:
                        continue
                    else:
                        poly = poly.polygon_sub(hole)
                        break

            raise gen.Return(poly)

class DatasetTracker(object):
    def __init__(self, 

@gen.coroutine
def dataset_tracker(query):


@gen.coroutine
def main():
    conn = yield open_connection()

    if "test" not in (yield r.table_list().run(conn)):
        yield r.table_create("test").run(conn)
    else:
        yield r.wait().run(conn)

    yield r.table("test").index_create("g", geo=True).run(conn)
    yield r.table("test").index_wait("g").run(conn)

    for i in range(100):
        g = yield random_geo(conn)
        j = yield g.to_geojson().run(conn)
        print repr(j)

if __name__ == '__main__':
    ioloop.IOLoop.current().run_sync(main)
