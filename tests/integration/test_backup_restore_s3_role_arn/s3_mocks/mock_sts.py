import sys
from datetime import datetime, timedelta, timezone
from bottle import request, response, route, run

if len(sys.argv) >= 3:
    expected_role = sys.argv[2]
else:
    expected_role = "miniorole"

if len(sys.argv) >= 4:
    expected_external_id = sys.argv[3]
else:
    expected_external_id = "miniexternalid"

if len(sys.argv) >= 5:
    expected_role_arn = sys.argv[4]
else:
    expected_role_arn = "arn::role"


@route("/")
def ping():
    response.content_type = "text/plain"
    response.set_header("Content-Length", 2)
    return "OK"


@route("/", method="POST")
def sts():
    access_key = "minio"
    secret_access_key = "wrong_key"

    # The parameters arrive form-encoded in the POST body, as the AWS SDK sends them.
    form = request.forms
    # The ARN names the role to assume. It is what a backup has to carry into its metadata for a chain
    # to reopen its base, so a request that arrives without it must not authenticate.
    role_arn_ok = form.get("RoleArn") == expected_role_arn
    role_ok = form.get("RoleSessionName") == expected_role
    # ExternalId is optional, but one that is sent has to match.
    external_id = form.get("ExternalId")
    external_id_ok = external_id is None or external_id == expected_external_id

    if role_arn_ok and role_ok and external_id_ok:
        secret_access_key = "ClickHouse_Minio_P@ssw0rd"

    expiration = datetime.now(timezone.utc) + timedelta(hours=1)
    expiration_str = expiration.strftime("%Y-%m-%dT%H:%M:%SZ")

    response.content_type = "text/xml"
    return f"""
        <AssumeRoleResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
            <AssumeRoleResult>
                <Credentials>
                    <AccessKeyId>{access_key}</AccessKeyId>
                    <SecretAccessKey>{secret_access_key}</SecretAccessKey>
                    <Expiration>{expiration_str}</Expiration>
                </Credentials>
            </AssumeRoleResult>
        </AssumeRoleResponse>
    """


run(host="0.0.0.0", port=int(sys.argv[1]))
