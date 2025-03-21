import sys

from bottle import response, route, run


MOCK_XML_RESPONSE = """<AssumeRoleResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
  <AssumeRoleResult>
    <Credentials>
      <AccessKeyId>minio</AccessKeyId>
      <SecretAccessKey>ClickHouse_Minio_P@ssw0rd</SecretAccessKey>
      <Expiration>2055-12-31T23:59:59Z</Expiration>
    </Credentials>
  </AssumeRoleResult>
</AssumeRoleResponse>"""

# <SessionToken>MOCK_SESSION_TOKEN</SessionToken>  is not being returned -- it is not required by minio
# When "real" STS returns it -- it is also used to read from s3

@route("/", method="POST")
def return_creds():
    response.status = 200
    response.content_type = "application/xml"
    return MOCK_XML_RESPONSE


@route("/", method="GET")
def ping():
    return "OK"


run(host="0.0.0.0", port=int(sys.argv[1]))