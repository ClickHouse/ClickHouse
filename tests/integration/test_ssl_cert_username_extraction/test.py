import base64
import os.path
import ssl
import urllib.parse
import urllib.request

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.ssl_context import WrapSSLContextWithSNI

# The test cluster is configured with certificate for that host name, see 'server-ext.cnf'.
# The client have to verify server certificate against that name. Client uses SNI
SSL_HOST = "integration-tests.clickhouse.com"
HTTPS_PORT = 8443
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

cluster = ClickHouseCluster(__file__)
node_cn = cluster.add_instance(
    "node_cn",
    main_configs=[
        "configs/ssl_config_cn.xml",
        "certs/server-key.pem",
        "certs/server-cert.pem",
        "certs/ca-cert.pem",
    ],
    user_configs=["configs/users.xml"],
)
node_san_uri = cluster.add_instance(
    "node_san_uri",
    main_configs=[
        "configs/ssl_config_san_uri.xml",
        "certs/server-key.pem",
        "certs/server-cert.pem",
        "certs/ca-cert.pem",
    ],
    user_configs=["configs/users.xml"],
)


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def get_ssl_context(cert_name):
    context = WrapSSLContextWithSNI(SSL_HOST, ssl.PROTOCOL_TLS_CLIENT)
    context.load_verify_locations(cafile=f"{SCRIPT_DIR}/certs/ca-cert.pem")
    if cert_name:
        context.load_cert_chain(
            f"{SCRIPT_DIR}/certs/{cert_name}-cert.pem",
            f"{SCRIPT_DIR}/certs/{cert_name}-key.pem",
        )
        context.verify_mode = ssl.CERT_REQUIRED
    context.check_hostname = True
    context.set_ciphers("DEFAULT")
    return context


def execute_query_https(node, query, cert_name=None, headers=None, params=None):
    url = f"https://{node.ip_address}:{HTTPS_PORT}/?query={urllib.parse.quote(query)}"
    if params:
        url += "&" + urllib.parse.urlencode(params)
    request = urllib.request.Request(url)
    for key, value in (headers or {}).items():
        request.add_header(key, value)
    response = urllib.request.urlopen(
        request, context=get_ssl_context(cert_name)
    ).read()
    return response.decode("utf-8")


def test_cert_only_authentication():
    assert (
        execute_query_https(node_cn, "SELECT currentUser()", cert_name="client1")
        == "client1\n"
    )


def test_explicit_credentials_take_precedence():
    # X-ClickHouse-User/X-ClickHouse-Key headers win over the certificate.
    assert (
        execute_query_https(
            node_cn,
            "SELECT currentUser()",
            cert_name="client1",
            headers={"X-ClickHouse-User": "jane", "X-ClickHouse-Key": "qwe123"},
        )
        == "jane\n"
    )

    # Query parameters win over the certificate.
    assert (
        execute_query_https(
            node_cn,
            "SELECT currentUser()",
            cert_name="client1",
            params={"user": "jane", "password": "qwe123"},
        )
        == "jane\n"
    )

    # The HTTP Basic 'Authorization' header wins over the certificate.
    basic = base64.b64encode(b"jane:qwe123").decode()
    assert (
        execute_query_https(
            node_cn,
            "SELECT currentUser()",
            cert_name="client1",
            headers={"Authorization": f"Basic {basic}"},
        )
        == "jane\n"
    )


def test_no_such_user():
    # The certificate is valid and the Common Name is extracted, but there is no user 'client5'.
    with pytest.raises(Exception) as err:
        execute_query_https(node_cn, "SELECT currentUser()", cert_name="client5")
    assert "403" in str(err.value)


def test_subject_check_is_not_bypassed():
    # The user 'client2' exists, but its configured ssl_certificate subject does not match the
    # certificate: extracting the user name from the certificate must not bypass the subject check.
    with pytest.raises(Exception) as err:
        execute_query_https(node_cn, "SELECT currentUser()", cert_name="client2")
    assert "403" in str(err.value)


def test_no_certificate_falls_back_to_default_user():
    assert execute_query_https(node_cn, "SELECT currentUser()") == "default\n"


def test_legacy_header_still_works():
    assert (
        execute_query_https(
            node_cn,
            "SELECT currentUser()",
            cert_name="client1",
            headers={
                "X-ClickHouse-SSL-Certificate-Auth": "on",
                "X-ClickHouse-User": "client1",
            },
        )
        == "client1\n"
    )


def test_san_uri_extraction():
    user_name = "spiffe://foo.com/bar"
    node_san_uri.query(
        f"CREATE USER '{user_name}' IDENTIFIED WITH ssl_certificate SAN 'URI:{user_name}'"
    )
    try:
        # 'client4-cert.pem' has the single Subject Alternative Name entry 'URI:spiffe://foo.com/bar'.
        assert (
            execute_query_https(
                node_san_uri, "SELECT currentUser()", cert_name="client4"
            )
            == f"{user_name}\n"
        )

        # 'client1-cert.pem' has no Subject Alternative Name entries, so there is no user name to
        # extract and the authentication fails.
        with pytest.raises(Exception) as err:
            execute_query_https(
                node_san_uri, "SELECT currentUser()", cert_name="client1"
            )
        assert "403" in str(err.value)
    finally:
        node_san_uri.query(f"DROP USER IF EXISTS '{user_name}'")
