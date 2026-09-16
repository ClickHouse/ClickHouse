import os.path
import ssl
import urllib.parse
import urllib.request

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.ssl_context import WrapSSLContextWithSNI

# The test cluster is configured with certificate for that host name, see 'server-ext.cnf'.
# The client has to verify server certificate against that name. Client uses SNI
SSL_HOST = "integration-tests.clickhouse.com"
HTTPS_PORT = 8443
POSTGRESQL_PORT = 5433
SECURE_NATIVE_PORT = 9440
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
MAX_RETRY = 5

ALLOWED_TLS13_SUITE = "TLS_AES_256_GCM_SHA384"
EXCLUDED_TLS13_SUITE = "TLS_CHACHA20_POLY1305_SHA256"

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "node",
    main_configs=[
        "configs/ssl_config.xml",
        "certs/server-key.pem",
        "certs/server-cert.pem",
        "certs/ca-cert.pem",
        "certs/dhparam4096.pem",
    ],
    user_configs=["configs/users_with_ssl_auth.xml"],
)
instance_with_suites = cluster.add_instance(
    "node_with_cipher_suites",
    main_configs=[
        "configs/ssl_config.xml",
        "configs/ssl_config_tls13_suites.xml",
        "certs/server-key.pem",
        "certs/server-cert.pem",
        "certs/ca-cert.pem",
        "certs/dhparam4096.pem",
    ],
    user_configs=["configs/users_with_ssl_auth.xml"],
)
instance_with_client_suites = cluster.add_instance(
    "node_with_client_cipher_suites",
    main_configs=[
        "configs/ssl_config.xml",
        "configs/ssl_config_tls13_client_suites.xml",
        "certs/server-key.pem",
        "certs/server-cert.pem",
        "certs/ca-cert.pem",
        "certs/dhparam4096.pem",
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
    assert "HTTP Error 403" in str(err.value)

    # TODO: Add non-flaky tests for:
    # - Wrong certificate: self-signed certificate.

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
    assert "HTTP Error 403" in str(err.value)

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


def offer_single_tls13_suite(node, suite):
    """Hand the HTTPS port exactly one TLS 1.3 cipher suite and report whether it was accepted.

    Reads the negotiated suite rather than the exit status, because s_client also reports a
    verification failure for the server certificate.
    """
    result = node.exec_in_container(
        [
            "bash",
            "-c",
            f"openssl s_client -connect 127.0.0.1:{HTTPS_PORT} -tls1_3 "
            f"-ciphersuites {suite} -brief </dev/null 2>&1 || true",
        ]
    )
    return f"Ciphersuite: {suite}" in result


def offer_single_tls13_suite_postgres(node, suite):
    """The same probe against the PostgreSQL port, which builds its own context.

    Reads the negotiated suite rather than the exit status, because s_client also reports a
    verification failure for the server certificate.
    """
    result = node.exec_in_container(
        [
            "bash",
            "-c",
            f"openssl s_client -starttls postgres -connect 127.0.0.1:{POSTGRESQL_PORT} "
            f"-tls1_3 -ciphersuites {suite} -brief </dev/null 2>&1 || true",
        ]
    )
    return f"Ciphersuite: {suite}" in result


def test_tls13_cipher_suites():
    # The excluded suite has to be available when the setting is absent, or its refusal on
    # the configured node would not be attributable to the setting.
    assert offer_single_tls13_suite(instance, EXCLUDED_TLS13_SUITE)
    assert offer_single_tls13_suite(instance_with_suites, ALLOWED_TLS13_SUITE)
    assert not offer_single_tls13_suite(instance_with_suites, EXCLUDED_TLS13_SUITE)


def test_tls13_cipher_suites_postgres_port():
    # The excluded suite has to be available when the setting is absent, or its refusal on
    # the configured node would not be attributable to the setting.
    assert offer_single_tls13_suite_postgres(instance, EXCLUDED_TLS13_SUITE)
    assert offer_single_tls13_suite_postgres(instance_with_suites, ALLOWED_TLS13_SUITE)
    assert not offer_single_tls13_suite_postgres(instance_with_suites, EXCLUDED_TLS13_SUITE)


def query_over_secure_native_port(node, target):
    """Query `target` over TLS from `node`, returning the answer and the error.

    `remoteSecure` connects through a `Poco::Net::SecureStreamSocket`, which takes the default
    client context, so the suites offered are the ones `openSSL.client` configures on `node`.
    """
    return node.query_and_get_answer_with_error(
        f"SELECT 1 FROM remoteSecure('{target.name}:{SECURE_NATIVE_PORT}', system.one)"
    )


def test_tls13_client_cipher_suites():
    # A client that configures no suites has to reach the same target, or the refusal below
    # would not be attributable to the client setting.
    answer, error = query_over_secure_native_port(instance, instance_with_suites)
    assert answer.strip() == "1", error

    # The configured client still reaches a server that offers the suite it asks for.
    answer, error = query_over_secure_native_port(instance_with_client_suites, instance)
    assert answer.strip() == "1", error

    # It offers only that suite, so a server restricted to another one is out of reach.
    answer, error = query_over_secure_native_port(
        instance_with_client_suites, instance_with_suites
    )
    assert answer.strip() != "1"
    assert "handshake failure" in error, error
