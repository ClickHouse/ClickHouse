import sys

from bottle import abort, route, run, request, response


@route("/redirected/<_path:path>")
def infinite_redirect(_path):
    response.set_header("Location", request.url)
    response.status = 307
    return "Redirected"


@route("/<_bucket>/<_path:path>")
def server(_bucket, _path):
    for name in request.headers:
        if name == "Authorization" and request.headers[name] == "Bearer TOKEN":
            return "1, 2, 3"
    response.status = 403
    response.content_type = "text/xml"
    return '<?xml version="1.0" encoding="UTF-8"?><Error><Code>ForbiddenError</Code><Message>Forbidden Error</Message><RequestId>txfbd566d03042474888193-00608d7537</RequestId></Error>'


@route("/")
def ping():
    return "OK"


run(host="0.0.0.0", port=int(sys.argv[1]))
