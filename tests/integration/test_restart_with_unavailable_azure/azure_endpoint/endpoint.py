#!/usr/bin/env python3
from bottle import request, response, route, run


@route("/<_bucket>/<_path:path>", ["GET", "POST", "PUT", "DELETE"])
def server(_bucket, _path):
    response.status = 403
    return "Forbidden"


@route("/")
def ping():
    return "OK"


run(host="0.0.0.0", port=8080)
