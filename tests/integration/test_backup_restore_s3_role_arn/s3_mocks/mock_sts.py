import sys
from datetime import datetime, timedelta, timezone
from urllib.parse import unquote

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

    # The ARN is percent-encoded in the query string, so match against the decoded form.
    url = unquote(str(request.url))
    # The ARN names the role to assume. It is what a backup has to carry into its metadata for a chain
    # to reopen its base, so a request that arrives without it must not authenticate.
    role_arn_ok = f"RoleArn={expected_role_arn}" in url
    role_ok = f"RoleSessionName={expected_role}" in url
    # ExternalId is optional, but one that is sent has to match.
    external_id_ok = ("ExternalId=" not in url) or (
        f"ExternalId={expected_external_id}" in url
    )

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
