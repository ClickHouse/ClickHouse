import os.path
import ssl
import urllib.parse
import urllib.request
import uuid
from os import remove

import pytest

from helpers.client import Client
from helpers.cluster import ClickHouseCluster
from helpers.ssl_context import WrapSSLContextWithSNI

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
        "configs/session_log.xml",
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
    config_path = f"{SCRIPT_DIR}/configs/client_{uuid.uuid4().hex}.xml"

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
        return client.query(query, user=user, password=password)
    finally:
        remove(config_path)


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

def test_mixed_x509_san_password_support():
    assert (
        execute_query_https("SELECT currentUser()", user="trurl", cert_name="client4")
        == "trurl\n"
    )
    assert (
        execute_query_https("SELECT currentUser()", enable_ssl_auth=False, user="trurl", password="mixed_sha_pass")
        == "trurl\n"
    )

    # Verify that system.users shows both auth methods (sha256_password + ssl_certificate)
    # for user 'trurl'. Sort by auth_type name for a deterministic assertion regardless of
    # config order. Both columns are extracted from a single sorted zip to keep them aligned.
    assert (
        instance.query(
            "SELECT name, "
            "arrayMap(x -> toString(x.1), s) AS auth_type, "
            "arrayMap(x -> x.2, s) AS auth_params "
            "FROM (SELECT name, arraySort(x -> toString(x.1), arrayZip(auth_type, auth_params)) AS s "
            "FROM system.users WHERE name='trurl')"
        )
        == 'trurl\t[\'sha256_password\',\'ssl_certificate\']\t[\'{}\',\'{"subject_alt_names":["URI:spiffe:\\\\/\\\\/foo.com\\\\/bar"]}\']\n'
    )
    

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


def test_x509_dns_san_wildcard_single_label():
    # A '*' in a DNS SAN must match exactly one DNS label (RFC 6125 6.4.3).
    # Positive: a single-label name under the wildcard authenticates, on both interfaces.
    assert (
        execute_query_native(
            instance, "SELECT currentUser()", user="wildcard_dns", cert_name="client7"
        )
        == "wildcard_dns\n"
    )
    assert (
        execute_query_https(
            "SELECT currentUser()", user="wildcard_dns", cert_name="client7"
        )
        == "wildcard_dns\n"
    )
    # Negative (authentication bypass): a multi-label name must NOT match a single-label
    # wildcard. 'evil.deep.corp.example.com' must be rejected by 'DNS:*.corp.example.com'.
    with pytest.raises(Exception) as err:
        execute_query_native(
            instance, "SELECT currentUser()", user="wildcard_dns", cert_name="client8"
        )
    assert "AUTHENTICATION_FAILED" in str(err.value)
    with pytest.raises(Exception) as err:
        execute_query_https(
            "SELECT currentUser()", user="wildcard_dns", cert_name="client8"
        )
    assert "403" in str(err.value)
    # Negative (empty label): the '*' must match a NON-empty label, so the malformed name
    # 'DNS:.corp.example.com' (empty first label) must be rejected by 'DNS:*.corp.example.com'.
    with pytest.raises(Exception) as err:
        execute_query_native(
            instance, "SELECT currentUser()", user="wildcard_dns", cert_name="client10"
        )
    assert "AUTHENTICATION_FAILED" in str(err.value)
    with pytest.raises(Exception) as err:
        execute_query_https(
            "SELECT currentUser()", user="wildcard_dns", cert_name="client10"
        )
    assert "403" in str(err.value)
    # Negative (slash in span): a DNS label contains no '/', so the matched span must reject one.
    # 'DNS:foo/bar.corp.example.com' must NOT match 'DNS:*.corp.example.com' (the old slash-count
    # guard forbade this; the single-label rule must keep forbidding it).
    with pytest.raises(Exception) as err:
        execute_query_native(
            instance, "SELECT currentUser()", user="wildcard_dns", cert_name="client12"
        )
    assert "AUTHENTICATION_FAILED" in str(err.value)
    with pytest.raises(Exception) as err:
        execute_query_https(
            "SELECT currentUser()", user="wildcard_dns", cert_name="client12"
        )
    assert "403" in str(err.value)


def test_x509_cn_wildcard_single_label():
    # The same single-label rule applies to a wildcard in the certificate CN.
    assert (
        execute_query_native(
            instance, "SELECT currentUser()", user="wildcard_cn", cert_name="client7"
        )
        == "wildcard_cn\n"
    )
    assert (
        execute_query_https(
            "SELECT currentUser()", user="wildcard_cn", cert_name="client7"
        )
        == "wildcard_cn\n"
    )
    with pytest.raises(Exception) as err:
        execute_query_native(
            instance, "SELECT currentUser()", user="wildcard_cn", cert_name="client8"
        )
    assert "AUTHENTICATION_FAILED" in str(err.value)
    with pytest.raises(Exception) as err:
        execute_query_https(
            "SELECT currentUser()", user="wildcard_cn", cert_name="client8"
        )
    assert "403" in str(err.value)
    # Negative (empty label): an empty CN label '.corp.example.com' must be rejected by the
    # CN wildcard '*.corp.example.com'.
    with pytest.raises(Exception) as err:
        execute_query_native(
            instance, "SELECT currentUser()", user="wildcard_cn", cert_name="client10"
        )
    assert "AUTHENTICATION_FAILED" in str(err.value)
    with pytest.raises(Exception) as err:
        execute_query_https(
            "SELECT currentUser()", user="wildcard_cn", cert_name="client10"
        )
    assert "403" in str(err.value)
    # Negative (slash in span): a CN label contains no '/', so 'foo/bar.corp.example.com' must NOT
    # match the CN wildcard '*.corp.example.com' (the old slash-count guard forbade this).
    with pytest.raises(Exception) as err:
        execute_query_native(
            instance, "SELECT currentUser()", user="wildcard_cn", cert_name="client12"
        )
    assert "AUTHENTICATION_FAILED" in str(err.value)
    with pytest.raises(Exception) as err:
        execute_query_https(
            "SELECT currentUser()", user="wildcard_cn", cert_name="client12"
        )
    assert "403" in str(err.value)


def test_x509_uri_san_wildcard_dot_in_segment():
    # Non-regression: '.' separates labels for DNS/CN but is NOT a separator for URI SANs,
    # whose separator is '/'. A wildcard URI path segment may legitimately contain dots, so
    # 'URI:spiffe://bar.com/foo/baz.qux/far' must still match 'URI:spiffe://bar.com/foo/*/far'.
    assert (
        execute_query_native(
            instance, "SELECT currentUser()", user="stewie", cert_name="client9"
        )
        == "stewie\n"
    )


def test_x509_unprefixed_san_wildcard_does_not_widen_uri():
    # A SAN wildcard configured without a recognized 'DNS:'/'URI:' prefix (here a bare 'SAN *')
    # must NOT widen URI matching. The label separator '.' is used only for a CN or a 'DNS:' SAN;
    # every other SAN keeps the '/' separator, whose "no '/' in the matched span" rule is identical
    # to the original slash-count guard. So 'SAN *' must NOT match the URI certificate
    # 'URI:spiffe://foo/bar' (client11), exactly as before this fix. Checked on both interfaces.
    with pytest.raises(Exception) as err:
        execute_query_native(
            instance,
            "SELECT currentUser()",
            user="wildcard_san_unprefixed",
            cert_name="client11",
        )
    assert "AUTHENTICATION_FAILED" in str(err.value)
    with pytest.raises(Exception) as err:
        execute_query_https(
            "SELECT currentUser()", user="wildcard_san_unprefixed", cert_name="client11"
        )
    assert "403" in str(err.value)


def test_x509_unprefixed_san_wildcard_does_not_span_dns_labels():
    # A SAN wildcard configured without a recognized 'DNS:'/'URI:' prefix must match nothing.
    # Certificate SAN subjects are always stored prefixed, so a bare 'SAN *.corp.example.com'
    # would otherwise let '*' absorb the candidate's 'DNS:' prefix and span DNS labels: against
    # 'DNS:evil.deep.corp.example.com' (client8) the matched span 'DNS:evil.deep' has no '/'.
    # The unprefixed SAN pattern must be rejected, so neither a single-label (client7) nor a
    # multi-label (client8) DNS certificate authenticates. Checked on both interfaces.
    for cert in ("client7", "client8"):
        with pytest.raises(Exception) as err:
            execute_query_native(
                instance,
                "SELECT currentUser()",
                user="wildcard_san_unprefixed_dns",
                cert_name=cert,
            )
        assert "AUTHENTICATION_FAILED" in str(err.value)
        with pytest.raises(Exception) as err:
            execute_query_https(
                "SELECT currentUser()",
                user="wildcard_san_unprefixed_dns",
                cert_name=cert,
            )
        assert "403" in str(err.value)


def test_session_log_certificate_success():
    # A successful certificate authentication must record the certificate details
    # in system.session_log, both for the native (TCP) and the HTTPS interface.
    instance.query("SYSTEM FLUSH LOGS")

    assert (
        execute_query_native(
            instance, "SELECT currentUser()", user="john", cert_name="client1"
        )
        == "john\n"
    )
    assert (
        execute_query_https("SELECT currentUser()", user="john", cert_name="client1")
        == "john\n"
    )

    instance.query("SYSTEM FLUSH LOGS")

    # A fully-populated certificate must be recorded for the successful login on both the native
    # (TCP) and the HTTPS interface, so the query must find such a LoginSuccess row for each.
    result = instance.query_with_retry(
        """
        SELECT count(DISTINCT interface)
        FROM system.session_log
        WHERE user = 'john' AND type = 'LoginSuccess' AND interface IN ('TCP', 'HTTP')
              AND has(certificate_subjects, 'CN:client1')
              AND certificate_issuer != '' AND certificate_serial != ''
              AND certificate_not_before IS NOT NULL AND certificate_not_after IS NOT NULL
              AND certificate_not_before < certificate_not_after
        """,
        check_callback=lambda r: r.strip() == "2",
    ).strip()
    assert result == "2", result


def test_session_log_certificate_login_failure():
    # 'john' may only authenticate with the 'client1' certificate. Presenting a different but
    # CA-valid certificate fails authentication, and the failed attempt must be recorded as a
    # LoginFailure carrying the presented certificate.
    instance.query("SYSTEM FLUSH LOGS")

    with pytest.raises(Exception):
        execute_query_native(instance, "SELECT 1", user="john", cert_name="client2")

    instance.query("SYSTEM FLUSH LOGS")

    result = instance.query_with_retry(
        """
        SELECT type, certificate_serial != ''
        FROM system.session_log
        WHERE user = 'john' AND has(certificate_subjects, 'CN:client2')
        ORDER BY event_time_microseconds DESC LIMIT 1 FORMAT TSV
        """,
        check_callback=lambda r: r.strip() != "",
    ).strip()
    assert result == "LoginFailure\t1", result


def test_session_log_certificate_https_non_cert_auth():
    # A client may present a TLS certificate over HTTPS while authenticating by another method
    # (here 'peter' authenticates without certificate authentication). The presented certificate
    # must still be recorded in system.session_log, even though it is not used for authentication.
    instance.query("SYSTEM FLUSH LOGS")

    assert (
        execute_query_https(
            "SELECT currentUser()",
            user="peter",
            enable_ssl_auth=False,
            cert_name="client1",
        )
        == "peter\n"
    )

    instance.query("SYSTEM FLUSH LOGS")

    # The login succeeded via a non-certificate method, but the presented certificate must still
    # be recorded with fully-populated metadata on the HTTPS (HTTP interface) LoginSuccess row.
    result = instance.query_with_retry(
        """
        SELECT count()
        FROM system.session_log
        WHERE user = 'peter' AND type = 'LoginSuccess' AND interface = 'HTTP'
              AND has(certificate_subjects, 'CN:client1')
              AND certificate_issuer != '' AND certificate_serial != ''
              AND certificate_not_before IS NOT NULL AND certificate_not_after IS NOT NULL
              AND certificate_not_before < certificate_not_after
        """,
        check_callback=lambda r: r.strip() not in ("", "0"),
    ).strip()
    assert result not in ("", "0"), result


def test_session_log_certificate_far_future_validity():
    # The 'client_far_future' certificate is valid until the year 2126, which is past the upper bound
    # of DateTime (UInt32 epoch seconds, ~2106). The validity period must be recorded faithfully and
    # not silently wrapped around, so the columns are DateTime64 rather than DateTime.
    instance.query("SYSTEM FLUSH LOGS")

    assert (
        execute_query_https(
            "SELECT currentUser()",
            user="peter",
            enable_ssl_auth=False,
            cert_name="client_far_future",
        )
        == "peter\n"
    )

    instance.query("SYSTEM FLUSH LOGS")

    # certificate_not_after must be recorded as a year well beyond 2106 (here 2126); with a DateTime
    # (UInt32) column the value would have wrapped around to before certificate_not_before instead.
    result = instance.query_with_retry(
        """
        SELECT toYear(certificate_not_after)
        FROM system.session_log
        WHERE user = 'peter' AND type = 'LoginSuccess' AND interface = 'HTTP'
              AND has(certificate_subjects, 'CN:client_far_future')
              AND certificate_not_before IS NOT NULL AND certificate_not_after IS NOT NULL
              AND certificate_not_before < certificate_not_after
              AND toYear(certificate_not_after) > 2106
        ORDER BY event_time_microseconds DESC LIMIT 1
        """,
        check_callback=lambda r: r.strip() not in ("", "0"),
    ).strip()
    assert result == "2126", result
