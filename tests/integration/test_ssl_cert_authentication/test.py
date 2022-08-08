import pytest
from helpers.cluster import ClickHouseCluster
from helpers.ssl_context import WrapSSLContextWithSNI
import urllib.request, urllib.parse
import ssl
import os.path

# The test cluster is configured with certificate for that host name, see 'server-cert.pem'.
# The client have to verify server certificate against that name. Client uses SNI
SSL_HOST = "integration-tests.clickhouse.com"
HTTPS_PORT = 8443
# It's important for the node to work at this IP because 'server-cert.pem' requires that (see server-ext.cnf).
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "node",
    main_configs=[
        "configs/ssl_config.xml",
        "certs/server-key.pem",
        "certs/server-cert.pem",
        "certs/ca-cert.pem",
    ],
    user_configs=["configs/users_with_ssl_auth.xml"],
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
    return context


def execute_query_https(
    query, user, enable_ssl_auth=True, cert_name=None, password=None
):
    url = f"https://{instance.ip_address}:{HTTPS_PORT}/?query={urllib.parse.quote(query)}"
    request = urllib.request.Request(url)
    request.add_header("X-ClickHouse-User", user)
    if enable_ssl_auth:
        request.add_header("X-ClickHouse-SSL-Certificate-Auth", "on")
    if password:
        request.add_header("X-ClickHouse-Key", password)
    response = urllib.request.urlopen(
        request, context=get_ssl_context(cert_name)
    ).read()
    return response.decode("utf-8")


def test_https():
    assert (
        execute_query_https("SELECT currentUser()", user="john", cert_name="client1")
        == "john\n"
    )
    assert (
        execute_query_https("SELECT currentUser()", user="lucy", cert_name="client2")
        == "lucy\n"
    )
    assert (
        execute_query_https("SELECT currentUser()", user="lucy", cert_name="client3")
        == "lucy\n"
    )


def test_https_wrong_cert():
    # Wrong certificate: different user's certificate
    with pytest.raises(Exception) as err:
        execute_query_https("SELECT currentUser()", user="john", cert_name="client2")
    assert "HTTP Error 403" in str(err.value)

    # Wrong certificate: self-signed certificate.
    with pytest.raises(Exception) as err:
        execute_query_https("SELECT currentUser()", user="john", cert_name="wrong")
    assert "unknown ca" in str(err.value)

    # No certificate.
    with pytest.raises(Exception) as err:
        execute_query_https("SELECT currentUser()", user="john")
    assert "HTTP Error 403" in str(err.value)

    # No header enabling SSL authentication.
    with pytest.raises(Exception) as err:
        execute_query_https(
            "SELECT currentUser()",
            user="john",
            enable_ssl_auth=False,
            cert_name="client1",
        )


def test_https_non_ssl_auth():
    # Users with non-SSL authentication are allowed, in this case we can skip sending a client certificate at all (because "verificationMode" is set to "relaxed").
    # assert execute_query_https("SELECT currentUser()", user="peter", enable_ssl_auth=False) == "peter\n"
    assert (
        execute_query_https(
            "SELECT currentUser()",
            user="jane",
            enable_ssl_auth=False,
            password="qwe123",
        )
        == "jane\n"
    )

    # But we still can send a certificate if we want.
    assert (
        execute_query_https(
            "SELECT currentUser()",
            user="peter",
            enable_ssl_auth=False,
            cert_name="client1",
        )
        == "peter\n"
    )
    assert (
        execute_query_https(
            "SELECT currentUser()",
            user="peter",
            enable_ssl_auth=False,
            cert_name="client2",
        )
        == "peter\n"
    )
    assert (
        execute_query_https(
            "SELECT currentUser()",
            user="peter",
            enable_ssl_auth=False,
            cert_name="client3",
        )
        == "peter\n"
    )

    assert (
        execute_query_https(
            "SELECT currentUser()",
            user="jane",
            enable_ssl_auth=False,
            password="qwe123",
            cert_name="client1",
        )
        == "jane\n"
    )
    assert (
        execute_query_https(
            "SELECT currentUser()",
            user="jane",
            enable_ssl_auth=False,
            password="qwe123",
            cert_name="client2",
        )
        == "jane\n"
    )
    assert (
        execute_query_https(
            "SELECT currentUser()",
            user="jane",
            enable_ssl_auth=False,
            password="qwe123",
            cert_name="client3",
        )
        == "jane\n"
    )

    # However if we send a certificate it must not be wrong.
    with pytest.raises(Exception) as err:
        execute_query_https(
            "SELECT currentUser()",
            user="peter",
            enable_ssl_auth=False,
            cert_name="wrong",
        )
    assert "unknown ca" in str(err.value)
    with pytest.raises(Exception) as err:
        execute_query_https(
            "SELECT currentUser()",
            user="jane",
            enable_ssl_auth=False,
            password="qwe123",
            cert_name="wrong",
        )
    assert "unknown ca" in str(err.value)


def test_create_user():
    instance.query("CREATE USER emma IDENTIFIED WITH ssl_certificate CN 'client3'")
    assert (
        execute_query_https("SELECT currentUser()", user="emma", cert_name="client3")
        == "emma\n"
    )
    assert (
        instance.query("SHOW CREATE USER emma")
        == "CREATE USER emma IDENTIFIED WITH ssl_certificate CN \\'client3\\'\n"
    )

    instance.query("ALTER USER emma IDENTIFIED WITH ssl_certificate CN 'client2'")
    assert (
        execute_query_https("SELECT currentUser()", user="emma", cert_name="client2")
        == "emma\n"
    )
    assert (
        instance.query("SHOW CREATE USER emma")
        == "CREATE USER emma IDENTIFIED WITH ssl_certificate CN \\'client2\\'\n"
    )

    with pytest.raises(Exception) as err:
        execute_query_https("SELECT currentUser()", user="emma", cert_name="client3")
    assert "HTTP Error 403" in str(err.value)

    assert (
        instance.query("SHOW CREATE USER lucy")
        == "CREATE USER lucy IDENTIFIED WITH ssl_certificate CN \\'client2\\', \\'client3\\'\n"
    )

    assert (
        instance.query(
            "SELECT name, auth_type, auth_params FROM system.users WHERE name IN ['emma', 'lucy'] ORDER BY name"
        )
        == 'emma\tssl_certificate\t{"common_names":["client2"]}\n'
        'lucy\tssl_certificate\t{"common_names":["client2","client3"]}\n'
    )
