#!/usr/bin/env python3
# Malicious S3 endpoint used to test that the AWS-SDK 301 redirect path in
# Client::getURIFromError validates the redirect target against RemoteHostFilter
# (SSRF protection), mirroring the Poco 307 path.
#
# Every S3 request is answered with 301 Moved Permanently whose Location points at
# this same container's IP address, reached under a name (the raw IP) that is NOT in
# <remote_url_allow_hosts>. A correctly-behaving server rejects that target with
# UNACCEPTABLE_URL before connecting. If the redirect were followed, the rewritten
# request would land on /forbidden_hit and flip the "followed" flag -- which the test
# asserts never happens.
import socket

from bottle import response, route, run

# Resolve our own container IP. It is reachable (same container) but is a different
# "host" from the allow-listed name "resolver", so the host filter must deny it.
OWN_IP = socket.gethostbyname(socket.gethostname())
REDIRECT_TARGET = OWN_IP + ":8080"

followed_redirect = {"hit": False}


@route("/forbidden_hit/<_path:path>", ["GET", "POST", "PUT", "HEAD", "DELETE"])
def forbidden(_path):
    # Reached only if ClickHouse followed the 301 to the disallowed target (the bug).
    followed_redirect["hit"] = True
    response.status = 200
    return "SHOULD_NOT_BE_REACHED"


@route("/followed")
def followed():
    return "YES" if followed_redirect["hit"] else "NO"


@route("/<_bucket>", ["GET", "POST", "PUT", "HEAD", "DELETE"])
@route("/<_bucket>/<_path:path>", ["GET", "POST", "PUT", "HEAD", "DELETE"])
def server(_bucket, _path=""):
    suffix = _bucket if not _path else _bucket + "/" + _path
    response.set_header("Location", "http://" + REDIRECT_TARGET + "/forbidden_hit/" + suffix)
    response.status = 301
    return "Redirected"


@route("/")
def ping():
    return "OK"


run(host="0.0.0.0", port=8080)
