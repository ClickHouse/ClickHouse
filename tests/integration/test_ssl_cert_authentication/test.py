import pytest
from helpers.client import Client
from helpers.cluster import ClickHouseCluster
from helpers.ssl_context import WrapSSLContextWithSNI
import urllib.request, urllib.parse
import ssl
import os.path
from os import remove
import logging


# The test cluster is configured with certificate for that host name, see 'server-ext.cnf'.
# The client have to verify server certificate against that name. Client uses SNI
SSL_HOST = "integration-tests.clickhouse.com"
HTTPS_PORT = 8443
# It's important for the node to work at this IP because 'server-cert.pem' requires that (see server-ext.cnf).
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
MAX_RETRY = 5

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


config = """<clickhouse>
    <openSSL>
        <client>
            <verificationMode>strict</verificationMode>
            <certificateFile>{certificateFile}</certificateFile>
            <privateKeyFile>{privateKeyFile}</privateKeyFile>
            <caConfig>{caConfig}</caConfig>
        </client>
    </openSSL>
</clickhouse>"""


def execute_query_native(node, query, user, cert_name, password=None):
    config_path = f"{SCRIPT_DIR}/configs/client.xml"

    formatted = config.format(
        certificateFile=f"{SCRIPT_DIR}/certs/{cert_name}-cert.pem",
        privateKeyFile=f"{SCRIPT_DIR}/certs/{cert_name}-key.pem",
        caConfig=f"{SCRIPT_DIR}/certs/ca-cert.pem",
    )

    file = open(config_path, "w")
    file.write(formatted)
    file.close()

    client = Client(
        node.ip_address,
        9440,
        command=cluster.client_bin_path,
        secure=True,
        config=config_path,
    )

    try:
        result = client.query(query, user=user, password=password)
        remove(config_path)
        return result
    except:
        remove(config_path)
        raise


def test_native():
    assert (
        execute_query_native(
            instance, "SELECT currentUser()", user="john", cert_name="client1"
        )
        == "john\n"
    )
    assert (
        execute_query_native(
            instance, "SELECT currentUser()", user="lucy", cert_name="client2"
        )
        == "lucy\n"
    )
    assert (
        execute_query_native(
            instance, "SELECT currentUser()", user="lucy", cert_name="client3"
        )
        == "lucy\n"
    )


def test_native_wrong_cert():
    # Wrong certificate: different user's certificate
    with pytest.raises(Exception) as err:
        execute_query_native(
            instance, "SELECT currentUser()", user="john", cert_name="client2"
        )
    assert "AUTHENTICATION_FAILED" in str(err.value)

    # Wrong certificate: self-signed certificate.
    # In this case clickhouse-client itself will throw an error
    with pytest.raises(Exception) as err:
        execute_query_native(
            instance, "SELECT currentUser()", user="john", cert_name="wrong"
        )
    assert "unknown ca" in str(err.value)


def test_native_fallback_to_password():
    # Unrelated certificate, correct password
    assert (
        execute_query_native(
            instance,
            "SELECT currentUser()",
            user="jane",
            cert_name="client2",
            password="qwe123",
        )
        == "jane\n"
    )

    # Unrelated certificate, wrong password
    with pytest.raises(Exception) as err:
        execute_query_native(
            instance,
            "SELECT currentUser()",
            user="jane",
            cert_name="client2",
            password="wrong",
        )
    assert "AUTHENTICATION_FAILED" in str(err.value)


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
    # Python 3.10 has removed many ciphers from the cipher suite.
    # Hence based on https://github.com/urllib3/urllib3/issues/3100#issuecomment-1671106236
    # we are expanding the list of cipher suites.
    context.set_ciphers("DEFAULT")
    return context


def execute_query_https(
    query, user, enable_ssl_auth=True, cert_name=None, password=None
):
    url = (
        f"https://{instance.ip_address}:{HTTPS_PORT}/?query={urllib.parse.quote(query)}"
    )
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
    assert "403" in str(err.value)

    # TODO: Add non-flaky tests for:
    # - Wrong certificate: self-signed certificate.

    # No certificate.
    with pytest.raises(Exception) as err:
        execute_query_https("SELECT currentUser()", user="john")
    assert "403" in str(err.value)

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

    # TODO: Add non-flaky tests for:
    # - sending wrong cert


def test_create_user():
    instance.query("DROP USER IF EXISTS emma")

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
    assert "403" in str(err.value)

    assert (
        instance.query("SHOW CREATE USER lucy")
        == "CREATE USER lucy IDENTIFIED WITH ssl_certificate CN \\'client2\\', \\'client3\\'\n"
    )

    assert (
        instance.query(
            "SELECT name, auth_type, auth_params FROM system.users WHERE name IN ['emma', 'lucy'] ORDER BY name"
        )
        == "emma\t['ssl_certificate']\t['{\"common_names\":[\"client2\"]}']\n"
        'lucy\t[\'ssl_certificate\']\t[\'{"common_names":["client2","client3"]}\']\n'
    )

    instance.query("DROP USER IF EXISTS emma")


def test_x509_san_support():
    instance.query("DROP USER IF EXISTS jemma")

    assert (
        execute_query_native(
            instance, "SELECT currentUser()", user="jerome", cert_name="client4"
        )
        == "jerome\n"
    )
    assert (
        execute_query_https("SELECT currentUser()", user="jerome", cert_name="client4")
        == "jerome\n"
    )
    assert (
        instance.query(
            "SELECT name, auth_type, auth_params FROM system.users WHERE name='jerome'"
        )
        == 'jerome\t[\'ssl_certificate\']\t[\'{"subject_alt_names":["URI:spiffe:\\\\/\\\\/foo.com\\\\/bar","URI:spiffe:\\\\/\\\\/foo.com\\\\/baz"]}\']\n'
    )
    # user `jerome` is configured via xml config, but `show create` should work regardless.
    assert (
        instance.query("SHOW CREATE USER jerome")
        == "CREATE USER jerome IDENTIFIED WITH ssl_certificate SAN \\'URI:spiffe://foo.com/bar\\', \\'URI:spiffe://foo.com/baz\\'\n"
    )

    instance.query(
        "CREATE USER jemma IDENTIFIED WITH ssl_certificate SAN 'URI:spiffe://foo.com/bar', 'URI:spiffe://foo.com/baz'"
    )
    assert (
        execute_query_https("SELECT currentUser()", user="jemma", cert_name="client4")
        == "jemma\n"
    )
    assert (
        instance.query("SHOW CREATE USER jemma")
        == "CREATE USER jemma IDENTIFIED WITH ssl_certificate SAN \\'URI:spiffe://foo.com/bar\\', \\'URI:spiffe://foo.com/baz\\'\n"
    )

    instance.query("DROP USER IF EXISTS jemma")


def test_x509_san_wildcard_support():
    assert (
        execute_query_native(
            instance, "SELECT currentUser()", user="stewie", cert_name="client5"
        )
        == "stewie\n"
    )

    assert (
        instance.query(
            "SELECT name, auth_type, auth_params FROM system.users WHERE name='stewie'"
        )
        == "stewie\t['ssl_certificate']\t['{\"subject_alt_names\":[\"URI:spiffe:\\\\/\\\\/bar.com\\\\/foo\\\\/*\\\\/far\"]}']\n"
    )

    assert (
        instance.query("SHOW CREATE USER stewie")
        == "CREATE USER stewie IDENTIFIED WITH ssl_certificate SAN \\'URI:spiffe://bar.com/foo/*/far\\'\n"
    )

    instance.query(
        "CREATE USER brian IDENTIFIED WITH ssl_certificate SAN 'URI:spiffe://bar.com/foo/*/far'"
    )

    assert (
        execute_query_https("SELECT currentUser()", user="brian", cert_name="client6")
        == "brian\n"
    )

    assert (
        instance.query("SHOW CREATE USER brian")
        == "CREATE USER brian IDENTIFIED WITH ssl_certificate SAN \\'URI:spiffe://bar.com/foo/*/far\\'\n"
    )

    instance.query("DROP USER brian")
