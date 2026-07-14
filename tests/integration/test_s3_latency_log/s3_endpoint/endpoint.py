#!/usr/bin/env python3
import time

# This HTTP server acts as a mock S3 endpoint.
# It's implemented in Python using the Bottle framework.
# This framework is single-threaded, so it can only handle one request at a time.
# All the sleeps will affect all consequent requests: https://www.bottlepy.org/docs/0.13/deployment.html
from bottle import request, response, route, run

total = 0


def throttle_and_count():
    global total
    total += 1
    time.sleep(0.5)


# Handle for MultipleObjectsDelete.
@route("/<_bucket>", ["POST"])
def delete(_bucket):
    throttle_and_count()
    response.set_header(
        "Location", "http://minio1:9001/" + _bucket + "?" + request.query_string
    )
    response.status = 307
    return "Redirected"


@route("/<_bucket>/<_path:path>", ["GET", "POST", "PUT", "DELETE"])
def server(_bucket, _path):
    throttle_and_count()
    response.set_header(
        "Location",
        "http://minio1:9001/" + _bucket + "/" + _path + "?" + request.query_string,
    )
    response.status = 307
    return "Redirected"


@route("/total", ["GET"])
def get_total():
    global total
    return str(total)


@route("/")
def ping():
    return "OK"


run(host="0.0.0.0", port=8080)
