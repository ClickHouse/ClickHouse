import sys
from datetime import datetime, timedelta, timezone

from bottle import request, response, route, run

access_key = "minio"


@route("/")
def ping():
    return "OK"


@route("/toggle", method="POST")
def toggle():
    global access_key
    access_key = "minio_rotated" if access_key == "minio" else "minio"
    return "OK"


@route("/", method="POST")
def assume_role():
    assert request.query.get("RoleArn") == "arn::role"
    secret = (
        "ClickHouse_Minio_P@ssw0rd"
        if access_key == "minio"
        else "Rotated_Minio_Test_Secret_123"
    )
    expiration = (datetime.now(timezone.utc) + timedelta(hours=1)).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    response.content_type = "text/xml"
    return f"""
<AssumeRoleResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
    <AssumeRoleResult>
        <Credentials>
            <AccessKeyId>{access_key}</AccessKeyId>
            <SecretAccessKey>{secret}</SecretAccessKey>
            <Expiration>{expiration}</Expiration>
        </Credentials>
    </AssumeRoleResult>
</AssumeRoleResponse>
"""


run(host="0.0.0.0", port=int(sys.argv[1]))
