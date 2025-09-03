import sys

from bottle import request, response, route, run


@route("/<_bucket>/<_path:path>")
def server(_bucket, _path):
    result = (
        request.headers["MyCustomHeader"]
        if "MyCustomHeader" in request.headers
        else "unknown"
    )
    response.content_type = "text/plain"
    response.set_header("Content-Length", len(result))
    return result


@route("/")
def ping():
    response.content_type = "text/plain"
    response.set_header("Content-Length", 2)
    return "OK"


run(host="0.0.0.0", port=int(sys.argv[1]))
