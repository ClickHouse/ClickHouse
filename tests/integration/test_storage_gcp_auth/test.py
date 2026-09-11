"""GCP authentication for S3/GCS access: the metadata-service source identity, and service account
impersonation on top of it -- the GCP counterpart of AWS STS `AssumeRole`.

For impersonation, ClickHouse obtains a source access token (from the GCP metadata service or an explicit ADC
triple), then exchanges it for a token that acts as `impersonate_service_account` by calling
`projects.serviceAccounts.generateAccessToken` on the IAM Service Account Credentials API.

Three mocks stand in for Google, all in the `resolver` container:

  metadata.py        the GCP metadata service, on port 80 -- ClickHouse builds that URL without a port, so the
                     two token behaviours tests need are keyed on the service account rather than on the port
  iamcredentials.py  `generateAccessToken`, which mints the impersonated token
  echo.py            the GCS XML endpoint, one instance per token it accepts

The GCS mock accepts only the token the test expects, so a request that skipped the exchange fails instead of
quietly succeeding.

The mocks deliberately do not imitate Google's validation or its error bodies: nothing here can confirm what the
real API accepts, so the suite asserts only on what ClickHouse itself decides -- which identity is used, whose
settings win, and what is refused before any request is made.
"""

import json
import logging
import os
import time

import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster

TOKEN_PATH = "computeMetadata/v1/instance/service-accounts"

# The rotating identity, whose token changes on every request: `test_gcp_auth` asserts on the refresh itself.
SERVICE_ACCOUNT = "my-account"
# The stable identity the impersonation tests exchange for a token of their target.
IMPERSONATION_SOURCE_ACCOUNT = "impersonation-source"
SOURCE_TOKEN = "source-token"
# What `iamcredentials.py` hands back for a successful exchange.
IMPERSONATED_TOKEN = "impersonated-token"

TARGET_SERVICE_ACCOUNT = "impersonated@test-project.iam.gserviceaccount.com"

GCS_PORT = 22234
IAM_CREDENTIALS_PORT = 22235
GCS_SOURCE_TOKEN_PORT = 22237
ROTATING_GCS_PORT = 22238

MOCKS = (
    # Accepts only the impersonated token, so a read that skipped the exchange fails.
    ("echo.py", [str(GCS_PORT), IMPERSONATED_TOKEN]),
    # Accepts the source token, for reads that do not impersonate.
    ("echo.py", [str(GCS_SOURCE_TOKEN_PORT), SOURCE_TOKEN]),
    # No token argument: the rotating mode, which `test_gcp_auth` needs.
    ("echo.py", [str(ROTATING_GCS_PORT)]),
    (
        "metadata.py",
        ["80", TOKEN_PATH, SERVICE_ACCOUNT, IMPERSONATION_SOURCE_ACCOUNT, SOURCE_TOKEN],
    ),
    ("iamcredentials.py", [str(IAM_CREDENTIALS_PORT), TARGET_SERVICE_ACCOUNT]),
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "node",
            main_configs=["configs/named_collections.xml"],
            user_configs=["configs/users.xml"],
            with_minio=True,
        )
        # A separate instance whose server `<s3>` config carries a per-endpoint `gcp_oauth`, so the bare-URL
        # `s3(url, extra_credentials(...))` form has a source identity to impersonate from.
        cluster.add_instance(
            "node_with_server_gcp_oauth",
            main_configs=["configs/s3_gcp_oauth.xml"],
            user_configs=["configs/users.xml"],
        )
        # Restricts outbound addresses to the GCS endpoint, so the IAM Credentials endpoint is not allowed.
        cluster.add_instance(
            "node_with_host_filter",
            main_configs=["configs/named_collections.xml", "configs/host_filter.xml"],
            user_configs=["configs/users.xml"],
        )
        # The server `<s3>` config carries impersonation qualifiers at the root, which apply to every endpoint.
        # A query cannot supply those, so they must not be treated as if it had.
        cluster.add_instance(
            "node_with_global_gcp_impersonation",
            main_configs=["configs/s3_gcp_oauth_global.xml"],
            user_configs=["configs/users.xml"],
        )
        # Keeps the restriction on for the default profile (which is what a startup table load runs under), so a
        # stored definition that overrode the impersonation settings is re-parsed under it on restart.
        cluster.add_instance(
            "node_queue_restricted",
            main_configs=["configs/named_collections.xml"],
            user_configs=["configs/users_restricted_default.xml"],
            with_zookeeper=True,
            stay_alive=True,
        )

        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        run_gcs_mocks(cluster)

        yield cluster
    finally:
        cluster.shutdown()


def run_gcs_mocks(cluster):
    logging.info("Starting gcs mocks")
    container_id = cluster.get_container_id("resolver")
    current_dir = os.path.dirname(__file__)

    for mock_filename, args in MOCKS:
        cluster.copy_file_to_container(
            container_id,
            os.path.join(current_dir, "gcs_mocks", mock_filename),
            mock_filename,
        )
        cluster.exec_in_container(
            container_id, ["python", mock_filename] + args, detach=True
        )

    for mock_filename, args in MOCKS:
        port = args[0]
        num_attempts = 100
        for attempt in range(num_attempts):
            ping_response = cluster.exec_in_container(
                container_id,
                ["curl", "-s", f"http://localhost:{port}/ping"],
                nothrow=True,
            )
            if ping_response == "OK":
                logging.debug(
                    f"mock {mock_filename} ({port}) answered {ping_response} on attempt {attempt}"
                )
                break
            if attempt == num_attempts - 1:
                assert ping_response == "OK", 'Expected "OK", but got "{}"'.format(
                    ping_response
                )
            time.sleep(1)

    logging.info("GCS mocks started")


def curl(started_cluster, path):
    return started_cluster.exec_in_container(
        started_cluster.get_container_id("resolver"),
        ["curl", "-s", f"http://localhost{path}"],
        nothrow=True,
    )


def reset_mocks(started_cluster):
    curl(started_cluster, f":{IAM_CREDENTIALS_PORT}/reset")


def num_impersonations(started_cluster, port=IAM_CREDENTIALS_PORT):
    return int(curl(started_cluster, f":{port}/counter"))


def last_impersonation_request(started_cluster, port=IAM_CREDENTIALS_PORT):
    return json.loads(curl(started_cluster, f":{port}/last_request"))


def test_gcp_auth(started_cluster):
    node = started_cluster.instances["node"]

    # Reset mock counters so the test is repeatable
    resolver_id = started_cluster.get_container_id("resolver")
    for port in [80, ROTATING_GCS_PORT]:
        started_cluster.exec_in_container(
            resolver_id,
            ["curl", "-s", f"http://localhost:{port}/reset"],
            nothrow=True,
        )

    def get_num_requests():
        count_response = started_cluster.exec_in_container(
            resolver_id,
            ["curl", "-s", "http://localhost/counter"],
            nothrow=True,
        )

        return int(count_response)

    node.query("DROP TABLE IF EXISTS s3_table")
    node.query(
        "CREATE TABLE s3_table (line String) ENGINE = S3(gcs_conn, filename='test.txt', format='LineAsString')"
    )

    assert get_num_requests() == 0
    assert node.query("SELECT * FROM s3_table") == "OK\n"

    # Wait to refresh token
    time.sleep(4)

    assert get_num_requests() == 2
    assert node.query("SELECT * FROM s3_table") == "OK\n"

    assert get_num_requests() == 4
    assert (
        node.query(
            "SELECT * FROM s3(gcs_conn, filename='test.txt', format='LineAsString')"
        )
        == "OK\n"
    )

    with pytest.raises(QueryRuntimeException) as ei:
        node.query(
            "SELECT * FROM s3(gcs_conn_bad, filename='test.txt', format='LineAsString')"
        )

    assert "AUTHENTICATION_FAILED" in ei.value.stderr


def test_impersonation_smoke(started_cluster):
    """A read succeeds only because the impersonated token -- not the source token -- reaches GCS."""
    node = started_cluster.instances["node"]
    reset_mocks(started_cluster)

    node.query("DROP TABLE IF EXISTS gcs_impersonated SYNC")
    node.query(
        "CREATE TABLE gcs_impersonated (line String) ENGINE = S3("
        "gcs_impersonation_conn, filename='test.txt', format='LineAsString')"
    )

    assert num_impersonations(started_cluster) == 0
    assert node.query("SELECT * FROM gcs_impersonated") == "OK\n"
    assert num_impersonations(started_cluster) > 0

    # The table function form resolves the same collection.
    assert (
        node.query(
            "SELECT * FROM s3(gcs_impersonation_conn, filename='test.txt', format='LineAsString')"
        )
        == "OK\n"
    )

    node.query("DROP TABLE gcs_impersonated SYNC")


def test_impersonation_requires_gcp_oauth(started_cluster):
    """Without `http_client = gcp_oauth` there is no source identity, so the setting must be rejected."""
    node = started_cluster.instances["node"]

    with pytest.raises(QueryRuntimeException) as ei:
        node.query(
            "SELECT * FROM s3(gcs_impersonation_without_oauth_conn, filename='test.txt', format='LineAsString')"
        )

    assert "BAD_ARGUMENTS" in ei.value.stderr
    assert "requires `http_client = gcp_oauth`" in ei.value.stderr


def test_impersonation_is_server_managed(started_cluster):
    """Impersonating from the server's metadata identity is a server-managed credential for a user query."""
    node = started_cluster.instances["node"]

    with pytest.raises(QueryRuntimeException) as ei:
        node.query(
            "SELECT * FROM s3(gcs_impersonation_conn, filename='test.txt', format='LineAsString')",
            user="restricted_user",
        )

    assert "ACCESS_DENIED" in ei.value.stderr
    assert "gcp_oauth" in ei.value.stderr
    assert "s3_allow_server_credentials_in_user_queries" in ei.value.stderr


def test_impersonation_via_extra_credentials(started_cluster):
    """`extra_credentials(impersonate_service_account = ...)` on top of a server-configured `gcp_oauth`.

    This is the closest analogue of the AWS `extra_credentials(role_arn = ...)` shape: the operator provisions
    the source identity in the server `<s3>` config, and the query names the account to impersonate.
    """
    node = started_cluster.instances["node_with_server_gcp_oauth"]
    reset_mocks(started_cluster)

    assert (
        node.query(
            "SELECT * FROM s3('http://resolver:22234/test/test.txt', 'LineAsString', "
            f"extra_credentials(impersonate_service_account = '{TARGET_SERVICE_ACCOUNT}', "
            "impersonation_scopes = 'https://www.googleapis.com/auth/devstorage.read_only'))"
        )
        == "OK\n"
    )

    request = last_impersonation_request(started_cluster)
    assert request["target"] == TARGET_SERVICE_ACCOUNT, request
    assert request["scope"] == [
        "https://www.googleapis.com/auth/devstorage.read_only"
    ], request


def test_impersonation_via_extra_credentials_is_restricted(started_cluster):
    """The same query is refused for a user who may not use the server's GCP identity as the source."""
    node = started_cluster.instances["node_with_server_gcp_oauth"]
    reset_mocks(started_cluster)

    with pytest.raises(QueryRuntimeException) as ei:
        node.query(
            "SELECT * FROM s3('http://resolver:22234/test/test.txt', 'LineAsString', "
            f"extra_credentials(impersonate_service_account = '{TARGET_SERVICE_ACCOUNT}'))",
            user="restricted_user",
        )

    # The server `gcp_oauth` is dropped for this user, which leaves the impersonation target without a source
    # identity -- reported explicitly rather than silently ignored and sent unimpersonated. The restriction is
    # what took the source away, so that is what is named: `extra_credentials` cannot supply a `gcp_oauth` of
    # its own, which makes the generic "requires `http_client = gcp_oauth`" advice unusable in this form.
    assert "ACCESS_DENIED" in ei.value.stderr
    assert "s3_allow_server_credentials_in_user_queries" in ei.value.stderr
    assert num_impersonations(started_cluster) == 0


def test_impersonation_target_from_query_is_not_impersonated_by_server(started_cluster):
    """A query-supplied target must not be impersonated using the collection's operator-provisioned identity."""
    node = started_cluster.instances["node"]
    reset_mocks(started_cluster)

    # `gcs_impersonation_conn` holds a metadata-service identity the query does not own. Overriding only the
    # target would impersonate an account of the caller's choosing with the server's identity as the source.
    with pytest.raises(QueryRuntimeException) as ei:
        node.query(
            "SELECT * FROM s3(gcs_impersonation_conn, filename='test.txt', format='LineAsString', "
            f"impersonate_service_account = '{TARGET_SERVICE_ACCOUNT}')",
            user="restricted_user",
        )

    assert "ACCESS_DENIED" in ei.value.stderr
    assert num_impersonations(started_cluster) == 0


@pytest.mark.parametrize(
    "override",
    [
        "iam_credentials_endpoint = 'http://resolver:22235'",
        "impersonation_scopes = 'https://www.googleapis.com/auth/cloud-platform'",
        "impersonation_delegates = 'other@test-project.iam.gserviceaccount.com'",
    ],
)
def test_impersonation_settings_from_query_are_refused(started_cluster, override):
    """Every setting that picks an identity is refused as a query override, not just the target.

    Redirecting `iam_credentials_endpoint` would send the collection's own access token to a chosen host, and
    widening `impersonation_scopes` would mint a token beyond what the operator provisioned.
    """
    node = started_cluster.instances["node"]
    reset_mocks(started_cluster)

    with pytest.raises(QueryRuntimeException) as ei:
        node.query(
            "SELECT * FROM s3(gcs_impersonation_conn, filename='test.txt', format='LineAsString', "
            f"{override})",
            user="restricted_user",
        )

    assert "ACCESS_DENIED" in ei.value.stderr
    assert "cannot be overridden" in ei.value.stderr
    assert "s3_allow_server_credentials_in_user_queries" in ei.value.stderr
    assert num_impersonations(started_cluster) == 0


def test_impersonation_lifetime_override_is_not_an_escalation(started_cluster):
    """`impersonation_lifetime_seconds` names no identity, so it is not refused as a query override.

    The minted token never leaves the server, and the target, the delegation chain, the scopes and the endpoint
    all stay the operator's, so there is nothing to escalate. Refusing it would also cost a stored definition
    that overrode only the lifetime its whole GCP block on the next restart.
    """
    node = started_cluster.instances["node"]
    reset_mocks(started_cluster)

    # Still refused for this user, but only for the reason that applies without any override at all: the
    # collection's source identity is server-managed.
    with pytest.raises(QueryRuntimeException) as ei:
        node.query(
            "SELECT * FROM s3(gcs_impersonation_conn, filename='test.txt', format='LineAsString', "
            "impersonation_lifetime_seconds = 60)",
            user="restricted_user",
        )

    assert "ACCESS_DENIED" in ei.value.stderr
    assert "cannot be overridden" not in ei.value.stderr
    assert num_impersonations(started_cluster) == 0

    # And it reaches the API for a user who may use that identity.
    assert (
        node.query(
            "SELECT * FROM s3(gcs_impersonation_conn, filename='test.txt', format='LineAsString', "
            "impersonation_lifetime_seconds = 60)"
        )
        == "OK\n"
    )
    assert last_impersonation_request(started_cluster)["lifetime"] == "60s"


def test_impersonation_settings_are_not_top_level_arguments(started_cluster):
    """A top-level impersonation argument would be dropped as an unknown key-value argument.

    The bare-URL form reads none of them, and an ignored `impersonate_service_account` would run the read with
    the source identity's own full-scope token while reporting success, so refuse it and say where it belongs.
    """
    node = started_cluster.instances["node_with_server_gcp_oauth"]
    reset_mocks(started_cluster)

    with pytest.raises(QueryRuntimeException) as ei:
        node.query(
            "SELECT * FROM s3('http://resolver:22234/test/test.txt', format = 'LineAsString', "
            f"impersonate_service_account = '{TARGET_SERVICE_ACCOUNT}')"
        )

    assert "BAD_ARGUMENTS" in ei.value.stderr
    assert "is not an argument of this form" in ei.value.stderr
    assert num_impersonations(started_cluster) == 0


def test_server_iam_credentials_endpoint_allows_plain_gcp_oauth(started_cluster):
    """A server-configured `iam_credentials_endpoint` must not break reads that do not impersonate."""
    node = started_cluster.instances["node_with_server_gcp_oauth"]
    reset_mocks(started_cluster)

    assert (
        node.query(
            f"SELECT * FROM s3('http://resolver:{GCS_SOURCE_TOKEN_PORT}/test/test.txt', 'LineAsString')"
        )
        == "OK\n"
    )
    assert num_impersonations(started_cluster) == 0


def test_impersonation_endpoint_honors_remote_host_filter(started_cluster):
    """The exchange carries the source identity's token, so its endpoint obeys the egress allow-list."""
    node = started_cluster.instances["node_with_host_filter"]
    reset_mocks(started_cluster)

    with pytest.raises(QueryRuntimeException) as ei:
        node.query(
            "SELECT * FROM s3(gcs_impersonation_conn, filename='test.txt', format='LineAsString')"
        )

    assert "UNACCEPTABLE_URL" in ei.value.stderr
    assert num_impersonations(started_cluster) == 0


def test_query_supplied_target_does_not_inherit_configured_qualifiers(started_cluster):
    """A target the query names must not be reached through the qualifiers configured for another one.

    `impersonation_delegates` and `impersonation_scopes` qualify the target they were configured alongside, so
    a query that supplies its own target supplies its own qualifiers too: otherwise the account it names would
    be impersonated through a delegation chain, and granted a scope set, that the operator provisioned for a
    different account entirely.
    """
    node = started_cluster.instances["node_with_global_gcp_impersonation"]
    reset_mocks(started_cluster)

    assert (
        node.query(
            f"SELECT * FROM s3('http://resolver:{GCS_PORT}/test/test.txt', 'LineAsString', "
            f"extra_credentials(impersonate_service_account = '{TARGET_SERVICE_ACCOUNT}'))"
        )
        == "OK\n"
    )

    request = last_impersonation_request(started_cluster)
    assert request["target"] == TARGET_SERVICE_ACCOUNT, request
    # The root-level `cloud-platform` scope and `root-delegate` belong to the operator's target, not this one,
    # so the exchange asks for the default scope and no delegation chain.
    assert request["scope"] == [
        "https://www.googleapis.com/auth/devstorage.read_write"
    ], request
    assert request["delegates"] is None, request


def test_query_supplied_scopes_win_over_configured_ones(started_cluster):
    """A query that narrows the scope of its own target must get the narrower token, not the configured one.

    The configured qualifiers are re-merged over the parsed settings when the object storage applies them, which
    used to put the operator's wider `cloud-platform` back on top of the scope the query asked for -- granting a
    broader token than the query requested, with nothing reported.
    """
    node = started_cluster.instances["node_with_global_gcp_impersonation"]
    reset_mocks(started_cluster)

    assert (
        node.query(
            f"SELECT * FROM s3('http://resolver:{GCS_PORT}/test/test.txt', 'LineAsString', "
            f"extra_credentials(impersonate_service_account = '{TARGET_SERVICE_ACCOUNT}', "
            "impersonation_scopes = 'https://www.googleapis.com/auth/devstorage.read_only'))"
        )
        == "OK\n"
    )

    request = last_impersonation_request(started_cluster)
    assert request["scope"] == [
        "https://www.googleapis.com/auth/devstorage.read_only"
    ], request
    assert request["delegates"] is None, request


def test_impersonation_qualifiers_require_a_target(started_cluster):
    """`impersonation_scopes` alone would be dropped silently, leaving a wider token than the query asked for."""
    node = started_cluster.instances["node_with_server_gcp_oauth"]
    reset_mocks(started_cluster)

    with pytest.raises(QueryRuntimeException) as ei:
        node.query(
            "SELECT * FROM s3('http://resolver:22234/test/test.txt', 'LineAsString', "
            "extra_credentials(impersonation_scopes = 'https://www.googleapis.com/auth/devstorage.read_only'))"
        )

    assert "BAD_ARGUMENTS" in ei.value.stderr
    assert "require `impersonate_service_account`" in ei.value.stderr
    assert num_impersonations(started_cluster) == 0


def test_global_impersonation_qualifiers_allow_plain_gcp_oauth(started_cluster):
    """Root-level qualifiers with no target in force must stay inert rather than fail the query.

    `impersonation_delegates` and `impersonation_scopes` only ever qualify a target, and every consumer
    downstream drops them without one, so a query that names no target has to keep reading with the source
    identity -- the same guarantee `test_server_iam_credentials_endpoint_allows_plain_gcp_oauth` makes for
    `iam_credentials_endpoint`.
    """
    node = started_cluster.instances["node_with_global_gcp_impersonation"]
    reset_mocks(started_cluster)

    assert (
        node.query(
            f"SELECT * FROM s3('http://resolver:{GCS_SOURCE_TOKEN_PORT}/test/test.txt', 'LineAsString')"
        )
        == "OK\n"
    )
    assert num_impersonations(started_cluster) == 0


def test_queue_with_query_overridden_impersonation_attaches_after_restart(
    started_cluster,
):
    """An `S3Queue` whose stored definition overrides the impersonation settings must still attach on restart.

    `S3Queue` parses its configuration before the storage object exists, so the flag that marks a load from
    existing metadata has to be set on the configuration first. Without it the re-parse at startup takes the
    `CREATE` path and refuses the definition, which aborts the attach and drops the queue out of
    `system.tables` -- while `s3_load_table_anonymously_if_credentials_restricted` promises the table is left
    in place with its queries failing instead.
    """
    node = started_cluster.instances["node_queue_restricted"]
    allow = {"s3_allow_server_credentials_in_user_queries": 1}

    node.query("DROP TABLE IF EXISTS q_impersonation SYNC")
    create = (
        "CREATE TABLE q_impersonation (line String) ENGINE = S3Queue("
        "gcs_impersonation_conn, format = 'LineAsString', "
        "impersonate_service_account = 'other@test-project.iam.gserviceaccount.com') "
        "SETTINGS mode = 'unordered', keeper_path = '/clickhouse/q_impersonation'"
    )

    # Refused without the opt-in: the override would be impersonated from the collection's own identity.
    assert "ACCESS_DENIED" in node.query_and_get_error(create)

    node.query(create, settings=allow)

    node.restart_clickhouse()

    assert node.query("SELECT 1").strip() == "1"
    assert "q_impersonation" in node.query("SHOW TABLES")

    node.query("DROP TABLE IF EXISTS q_impersonation SYNC")
