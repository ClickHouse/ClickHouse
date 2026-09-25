"""
Transport-level tests for the LDAP client: LDAPS, StartTLS, TLS protocol version bounds,
certificate verification and the connection timeouts
(https://github.com/ClickHouse/ClickHouse/issues/73474).

Every LDAP server definition in `configs/ldap_servers.xml` points at the shared `openldap`
fixture and binds as the same directory entry, `janedoe`; only the transport settings differ.
The local users in `configs/users.xml` each authenticate through one of those definitions, so a
single ClickHouse instance covers every variant, and the user name in the server log tells the
variants apart.

The client always receives the generic `Authentication failed` message, so the tests for the
failure cases look at the reason logged by `AccessControl::authenticate` in the server log.
"""

import logging
import re
import time

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.fake_ldap import start_fake_ldap_server
from helpers.test_tools import TSV, assert_logs_contain_with_retry

LDAP_ADMIN_BIND_DN = "cn=admin,dc=example,dc=org"
LDAP_ADMIN_PASSWORD = "clickhouse"
LDAP_PASSWORD = "qwerty"

cluster = ClickHouseCluster(__file__)

# The CA that issued the fixture's server certificate is taken from the runner image sources,
# so that there is a single copy to keep in sync with `generate.sh`.
node = cluster.add_instance(
    "node",
    main_configs=[
        "configs/ldap_servers.xml",
        "../../../ci/docker/integration/runner/misc/openldap/certs/ca.pem",
    ],
    user_configs=["configs/users.xml"],
    with_ldap=True,
)


def wait_ldaps_ready(timeout=180):
    """The shared readiness check only talks to the plain port; make sure the LDAPS listener
    answers with a certificate that verifies against the fixture CA before running the tests.
    libldap checks `localhost` against the machine hostname, hence the IP literal."""
    start = time.time()
    while time.time() - start < timeout:
        try:
            cluster.exec_in_container(
                cluster.ldap_id,
                [
                    "bash",
                    "-c",
                    "LDAPTLS_CACERT=/certs/ca.pem /opt/bitnami/openldap/bin/ldapsearch -x"
                    f" -H ldaps://127.0.0.1:{cluster.ldap_tls_port}"
                    f" -D {LDAP_ADMIN_BIND_DN} -w {LDAP_ADMIN_PASSWORD} -b dc=example,dc=org"
                    " -s base dn",
                ],
                user="root",
            )
            logging.info("LDAPS is ready")
            return
        except Exception as ex:
            logging.debug("LDAPS not ready yet: %s", ex)
            time.sleep(1)
    raise Exception("Timed out waiting for the LDAPS listener")


# Served by `helpers/fake_ldap_server.py` inside the node, see `stalled_bind` and `stalled_search`
# in `configs/ldap_servers.xml`.
FAKE_LDAP_SERVERS = {3890: "bind", 3891: "search"}


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        wait_ldaps_ready()
        for port, mode in FAKE_LDAP_SERVERS.items():
            start_fake_ldap_server(node, port, mode)
        yield cluster
    finally:
        cluster.shutdown()


def failed_logins_in_log(user):
    """Returns the `Authentication failed` lines that `AccessControl::authenticate` logged for
    `user`, each of which carries the underlying exception. The log is written asynchronously
    with respect to the client's error, so wait for the line to appear first."""
    pattern = f"user: {user}: Authentication failed"
    assert_logs_contain_with_retry(node, pattern)
    return [line for line in node.grep_in_log(pattern).splitlines() if line.strip()]


def assert_login_works(user):
    assert node.query("SELECT currentUser()", user=user, password=LDAP_PASSWORD) == TSV(
        [[user]]
    )


def assert_login_fails(user, password=LDAP_PASSWORD):
    error = node.query_and_get_error("SELECT 1", user=user, password=password)
    assert f"{user}: Authentication failed" in error, error
    return failed_logins_in_log(user)


def test_ldaps_with_pinned_ca():
    assert_login_works("user_ldaps")

    # A wrong password over TLS is an ordinary rejection, not a transport error.
    lines = assert_login_fails("user_ldaps", password="wrong")
    assert not any("LDAP_ERROR" in line for line in lines), lines


def assert_negotiated(server, expected):
    """`LDAPClient` logs the negotiated protocol version and cipher on the first bind of a protected
    connection; it is the only observable proof of the transport mode and of the protocol bounds.
    """
    assert_logs_contain_with_retry(
        node, f"LDAP server '{server}': the connection to openldap:{expected}"
    )


def test_tls13_minimum():
    assert_login_works("user_ldaps_tls13")
    assert_negotiated("ldaps_tls13", "1636 is protected by LDAPS (TLSv1.3 with cipher")


def test_tls12_maximum():
    assert_login_works("user_ldaps_max_tls12")
    assert_negotiated(
        "ldaps_max_tls12", "1636 is protected by LDAPS (TLSv1.2 with cipher"
    )


def test_tls11_maximum_fails_handshake():
    # `tls_maximum_protocol_version` is applied to the TLS context, so capping it below what the
    # server accepts makes the handshake fail with a transport error.
    lines = assert_login_fails("user_ldaps_max_tls11")
    assert any(
        "LDAP_ERROR" in line and "Can't contact LDAP server" in line for line in lines
    ), lines
    assert any(re.search("TLS|SSL", line) for line in lines), lines


def test_demand_without_trusted_ca_fails():
    lines = assert_login_fails("user_ldaps_no_ca")
    assert any(
        "LDAP_ERROR" in line and "Can't contact LDAP server" in line for line in lines
    ), lines
    assert any("certificate verify failed" in line for line in lines), lines


def test_never_skips_certificate_verification():
    assert_login_works("user_ldaps_never")


def test_starttls_on_plain_port():
    assert_login_works("user_starttls")
    # Plain binds are accepted on this port, so the success alone would not prove the upgrade.
    assert_negotiated("starttls", "1389 is protected by StartTLS (TLSv1.")


def test_network_timeout_bounds_unreachable_server():
    # The address is not routable, so the SYN is dropped and the connect attempt can only end
    # through `network_timeout` (1 second here). Without the setting taking effect the attempt
    # would last for the default 30 seconds, see issue 73474. The bound is deliberately wide.
    start = time.monotonic()
    lines = assert_login_fails("user_unreachable")
    elapsed = time.monotonic() - start
    assert elapsed < 15, f"authentication took {elapsed:.1f}s"
    assert any(
        "LDAP_ERROR" in line and "Can't contact LDAP server" in line for line in lines
    ), lines


def test_operation_timeout_bounds_a_stalled_bind():
    """The fake server accepts the connection and never answers, so only `operation_timeout`
    (1 second) can end the wait for the bind result; `network_timeout` and `search_timeout` are
    20 seconds here and the libldap default would be 40. The bound is deliberately wide.
    """
    start = time.monotonic()
    lines = assert_login_fails("user_stalled_bind")
    elapsed = time.monotonic() - start
    assert elapsed < 15, f"authentication took {elapsed:.1f}s"
    assert any("LDAP_ERROR" in line and "Timed out" in line for line in lines), lines


def test_search_timeout_bounds_a_stalled_search():
    """The fake server answers the bind and never the `user_dn_detection` search that follows, so
    only `search_timeout` (1 second) can end the wait: `operation_timeout` does not apply to
    searches and is 20 seconds here, like `network_timeout`; without the client-side limit the
    search would wait for the server forever."""
    start = time.monotonic()
    lines = assert_login_fails("user_stalled_search")
    elapsed = time.monotonic() - start
    assert elapsed < 15, f"authentication took {elapsed:.1f}s"
    assert any("LDAP_ERROR" in line and "Timed out" in line for line in lines), lines


def test_maximum_below_minimum_is_rejected_at_parse_time():
    # `.` stands for the backquotes around the server name; a literal backquote would be
    # interpreted by the shell that runs grep.
    assert node.contains_in_log("Could not parse LDAP server .bad_tls_range.")
    assert node.contains_in_log(
        "Bad value for 'tls_maximum_protocol_version' entry, "
        "must not be lower than 'tls_minimum_protocol_version'"
    )

    # The rejected definition is not usable for authentication.
    lines = assert_login_fails("user_bad_tls_range")
    assert any(
        "bad_tls_range" in line
        and ("is not configured" in line or "misconfigured" in line)
        for line in lines
    ), lines


def test_zero_timeout_is_rejected_at_parse_time():
    assert node.contains_in_log("Could not parse LDAP server .bad_timeout.")
    assert node.contains_in_log(
        "Bad value for 'network_timeout' entry, must be a number of seconds between 1 and"
    )

    lines = assert_login_fails("user_bad_timeout")
    assert any(
        "bad_timeout" in line
        and ("is not configured" in line or "misconfigured" in line)
        for line in lines
    ), lines
