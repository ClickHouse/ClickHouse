#!/usr/bin/env python3
# Malicious S3 endpoint used to test that the AWS-SDK 301 redirect path in
# Client::doRequest validates the redirect target against RemoteHostFilter
# (SSRF protection), mirroring the Poco 307 path.
#
# Most S3 requests are answered with 301 Moved Permanently whose Location points at
# this same container's IP address, reached under a name (the raw IP) that is NOT in
# <remote_url_allow_hosts>. A correctly-behaving server rejects that target with
# UNACCEPTABLE_URL before connecting. If the redirect were followed, the rewritten
# request would land on /forbidden_hit and flip the "followed" flag -- which the test
# asserts never happens. The `cache` and `head` buckets redirect to an allow-listed alias;
# `virtual` redirects to a disallowed bucket host whose normalized endpoint is allow-listed.
import socket

from bottle import request, response, route, run

# Resolve our own container IP. It is reachable (same container) but is a different
# "host" from the allow-listed name "resolver", so the host filter must deny it.
OWN_IP = socket.gethostbyname(socket.gethostname())
REDIRECT_TARGET = OWN_IP + ":8080"
ALLOWED_REDIRECT_TARGET = "redirected:8080"
VIRTUAL_HOSTED_REDIRECT_TARGET = "bucket.s3.resolver:8080"

followed_redirect = {"hit": False}
initial_requests = {"cache": 0}


@route("/forbidden_hit/<_path:path>", ["GET", "POST", "PUT", "HEAD", "DELETE"])
def forbidden(_path):
    # Reached only if ClickHouse followed the 301 to the disallowed target (the bug).
    followed_redirect["hit"] = True
    response.status = 200
    return "SHOULD_NOT_BE_REACHED"


@route("/followed")
def followed():
    return "YES" if followed_redirect["hit"] else "NO"


@route("/initial_requests/<bucket>")
def get_initial_requests(bucket):
    return str(initial_requests.get(bucket, 0))


@route("/<_bucket>", ["GET", "POST", "PUT", "HEAD", "DELETE"])
@route("/<_bucket>/<_path:path>", ["GET", "POST", "PUT", "HEAD", "DELETE"])
def server(_bucket, _path=""):
    if request.urlparts.netloc == ALLOWED_REDIRECT_TARGET:
        if _bucket == "head" and _path:
            response.set_header("ETag", '"etag"')
            return "1\n"

        response.status = 403
        response.content_type = "application/xml"
        return "<Error><Code>AccessDenied</Code><Message>AccessDenied</Message></Error>"

    if _bucket in initial_requests:
        initial_requests[_bucket] += 1

    suffix = _bucket if not _path else _bucket + "/" + _path
    if _bucket in ("cache", "head"):
        target = ALLOWED_REDIRECT_TARGET
        target_path = suffix
    elif _bucket == "virtual":
        target = VIRTUAL_HOSTED_REDIRECT_TARGET
        target_path = "forbidden_hit/" + suffix
    else:
        target = REDIRECT_TARGET
        target_path = "forbidden_hit/" + suffix
    response.set_header("Location", "http://" + target + "/" + target_path)
    response.status = 301
    response.content_type = "application/xml"
    return "<Error><Code>PermanentRedirect</Code><Endpoint>{}</Endpoint></Error>".format(target)


@route("/")
def ping():
    return "OK"


run(host="0.0.0.0", port=8080)
