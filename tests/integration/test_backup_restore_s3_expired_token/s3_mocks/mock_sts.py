import sys

from bottle import request, response, route, run

credential_sets = None
credential_index = 0


@route("/")
def ping():
    return "OK"


@route("/toggle", method="POST")
def toggle():
    global credential_index
    if credential_sets is None:
        response.status = 503
        return "Credentials are not configured"
    credential_index = 1 - credential_index
    return "OK"


@route("/set", method="POST")
def set_credentials():
    global credential_sets, credential_index
    configured = request.json["credentials"]
    assert len(configured) == 2
    credential_sets = configured
    credential_index = 0
    return "OK"


@route("/", method="POST")
def assume_role():
    assert request.query.get("RoleArn") == "arn::role"
    if credential_sets is None:
        response.status = 503
        return "Credentials are not configured"
    credentials = credential_sets[credential_index]
    response.content_type = "text/xml"
    return f"""
<AssumeRoleResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
    <AssumeRoleResult>
        <Credentials>
            <AccessKeyId>{credentials['AccessKeyId']}</AccessKeyId>
            <SecretAccessKey>{credentials['SecretAccessKey']}</SecretAccessKey>
            <SessionToken>{credentials['SessionToken']}</SessionToken>
            <Expiration>{credentials['Expiration']}</Expiration>
        </Credentials>
    </AssumeRoleResult>
</AssumeRoleResponse>
"""


run(host="0.0.0.0", port=int(sys.argv[1]))
