#!/usr/bin/env python3
from bottle import request, response, route, run


# Handle for MultipleObjectsDelete.
@route("/<_bucket>", ["POST"])
def delete(_bucket):
    response.set_header(
        "Location", "http://minio1:9001/" + _bucket + "?" + request.query_string
    )
    response.status = 307
    return "Redirected"


@route("/<_bucket>/<_path:path>", ["GET", "POST", "PUT", "DELETE"])
def server(_bucket, _path):
    # CompleteMultipartUpload request
    # We always returning 200 + error in body to simulate: https://aws.amazon.com/premiumsupport/knowledge-center/s3-resolve-200-internalerror/
    if request.query_string.startswith("uploadId="):
        response.status = 200
        response.content_type = "text/xml"
        return '<?xml version="1.0" encoding="UTF-8"?><Error><Code>InternalError</Code><Message>We encountered an internal error. Please try again.</Message><RequestId>txfbd566d03042474888193-00608d7538</RequestId></Error>'

    response.set_header(
        "Location",
        "http://minio1:9001/" + _bucket + "/" + _path + "?" + request.query_string,
    )
    response.status = 307
    return "Redirected"


@route("/")
def ping():
    return "OK"


run(host="0.0.0.0", port=8080)
