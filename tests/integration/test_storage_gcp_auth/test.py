"""GCP metadata authentication and service account impersonation.

The GCS mock accepts only the expected token, so skipping the IAM exchange fails a read.
The IAM mock records requests; it does not reproduce Google's validation rules.
"""

import json
import logging
import os
import time

import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import wait_condition

TOKEN_PATH = "computeMetadata/v1/instance/service-accounts"

SERVICE_ACCOUNT = "my-account"
IMPERSONATION_SOURCE_ACCOUNT = "impersonation-source"
SOURCE_TOKEN = "source-token"
IMPERSONATED_TOKEN = "impersonated-token"

TARGET_SERVICE_ACCOUNT = "impersonated@test-project.iam.gserviceaccount.com"

GCS_PORT = 22234
IAM_CREDENTIALS_PORT = 22235
GCS_SOURCE_TOKEN_PORT = 22237
ROTATING_GCS_PORT = 22238

MOCKS = (
    ("echo.py", [str(GCS_PORT), IMPERSONATED_TOKEN]),
    ("echo.py", [str(GCS_SOURCE_TOKEN_PORT), SOURCE_TOKEN]),
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
        cluster.add_instance(
            "node_with_server_gcp_oauth",
            main_configs=["configs/s3_gcp_oauth.xml"],
            user_configs=["configs/users.xml"],
        )
        cluster.add_instance(
            "node_with_host_filter",
            main_configs=["configs/named_collections.xml", "configs/host_filter.xml"],
            user_configs=["configs/users.xml"],
        )
        cluster.add_instance(
            "node_with_global_gcp_impersonation",
            main_configs=["configs/s3_gcp_oauth_global.xml"],
            user_configs=["configs/users.xml"],
        )
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
        wait_condition(
            lambda: cluster.exec_in_container(
                container_id,
                ["curl", "-s", f"http://localhost:{port}/ping"],
                nothrow=True,
            ),
            lambda response: response == "OK",
            max_attempts=100,
            delay=1,
        )

    logging.info("GCS mocks started")


def curl(started_cluster, path):
    return started_cluster.exec_in_container(
        started_cluster.get_container_id("resolver"),
        ["curl", "-s", f"http://localhost{path}"],
        nothrow=True,
    )


def reset_mocks(started_cluster):
    curl(started_cluster, f":{IAM_CREDENTIALS_PORT}/reset")


def num_impersonations(started_cluster):
    return int(curl(started_cluster, f":{IAM_CREDENTIALS_PORT}/counter"))


def last_impersonation_request(started_cluster):
    return json.loads(curl(started_cluster, f":{IAM_CREDENTIALS_PORT}/last_request"))


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

    assert (
        node.query(
            "SELECT * FROM s3(gcs_impersonation_conn, filename='test.txt', format='LineAsString')"
        )
        == "OK\n"
    )

    node.query("DROP TABLE gcs_impersonated SYNC")


def test_impersonation_requires_gcp_oauth(started_cluster):
    node = started_cluster.instances["node"]

    with pytest.raises(QueryRuntimeException) as ei:
        node.query(
            "SELECT * FROM s3(gcs_impersonation_without_oauth_conn, filename='test.txt', format='LineAsString')"
        )

    assert "BAD_ARGUMENTS" in ei.value.stderr
    assert "requires `http_client = gcp_oauth`" in ei.value.stderr


def test_impersonation_is_server_managed(started_cluster):
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
    node = started_cluster.instances["node_with_server_gcp_oauth"]
    reset_mocks(started_cluster)

    with pytest.raises(QueryRuntimeException) as ei:
        node.query(
            "SELECT * FROM s3('http://resolver:22234/test/test.txt', 'LineAsString', "
            f"extra_credentials(impersonate_service_account = '{TARGET_SERVICE_ACCOUNT}'))",
            user="restricted_user",
        )

    assert "ACCESS_DENIED" in ei.value.stderr
    assert "s3_allow_server_credentials_in_user_queries" in ei.value.stderr
    assert num_impersonations(started_cluster) == 0


@pytest.mark.parametrize(
    "override",
    [
        f"impersonate_service_account = '{TARGET_SERVICE_ACCOUNT}'",
        "iam_credentials_endpoint = 'http://resolver:22235'",
        "impersonation_scopes = 'https://www.googleapis.com/auth/cloud-platform'",
        "impersonation_delegates = 'other@test-project.iam.gserviceaccount.com'",
    ],
)
def test_impersonation_settings_from_query_are_refused(started_cluster, override):
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
    node = started_cluster.instances["node"]
    reset_mocks(started_cluster)

    with pytest.raises(QueryRuntimeException) as ei:
        node.query(
            "SELECT * FROM s3(gcs_impersonation_conn, filename='test.txt', format='LineAsString', "
            "impersonation_lifetime_seconds = 60)",
            user="restricted_user",
        )

    assert "ACCESS_DENIED" in ei.value.stderr
    assert "cannot be overridden" not in ei.value.stderr
    assert num_impersonations(started_cluster) == 0

    assert (
        node.query(
            "SELECT * FROM s3(gcs_impersonation_conn, filename='test.txt', format='LineAsString', "
            "impersonation_lifetime_seconds = 60)"
        )
        == "OK\n"
    )
    assert last_impersonation_request(started_cluster)["lifetime"] == "60s"


def test_impersonation_settings_are_not_top_level_arguments(started_cluster):
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


@pytest.mark.parametrize(
    "node_name",
    ["node_with_server_gcp_oauth", "node_with_global_gcp_impersonation"],
)
def test_configured_impersonation_allows_plain_gcp_oauth(started_cluster, node_name):
    node = started_cluster.instances[node_name]
    reset_mocks(started_cluster)

    assert (
        node.query(
            f"SELECT * FROM s3('http://resolver:{GCS_SOURCE_TOKEN_PORT}/test/test.txt', 'LineAsString')"
        )
        == "OK\n"
    )
    assert num_impersonations(started_cluster) == 0


def test_impersonation_endpoint_honors_remote_host_filter(started_cluster):
    node = started_cluster.instances["node_with_host_filter"]
    reset_mocks(started_cluster)

    with pytest.raises(QueryRuntimeException) as ei:
        node.query(
            "SELECT * FROM s3(gcs_impersonation_conn, filename='test.txt', format='LineAsString')"
        )

    assert "UNACCEPTABLE_URL" in ei.value.stderr
    assert num_impersonations(started_cluster) == 0


@pytest.mark.parametrize(
    "scopes, expected_scope",
    [
        ("", "https://www.googleapis.com/auth/devstorage.read_write"),
        (
            ", impersonation_scopes = 'https://www.googleapis.com/auth/devstorage.read_only'",
            "https://www.googleapis.com/auth/devstorage.read_only",
        ),
    ],
)
def test_query_target_uses_its_own_qualifiers(started_cluster, scopes, expected_scope):
    node = started_cluster.instances["node_with_global_gcp_impersonation"]
    reset_mocks(started_cluster)

    assert (
        node.query(
            f"SELECT * FROM s3('http://resolver:{GCS_PORT}/test/test.txt', 'LineAsString', "
            f"extra_credentials(impersonate_service_account = '{TARGET_SERVICE_ACCOUNT}'{scopes}))"
        )
        == "OK\n"
    )

    request = last_impersonation_request(started_cluster)
    assert request["target"] == TARGET_SERVICE_ACCOUNT, request
    assert request["scope"] == [expected_scope], request
    assert request["delegates"] is None, request


def test_impersonation_qualifiers_require_a_target(started_cluster):
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


def test_queue_with_query_overridden_impersonation_attaches_after_restart(
    started_cluster,
):
    node = started_cluster.instances["node_queue_restricted"]
    allow = {"s3_allow_server_credentials_in_user_queries": 1}

    node.query("DROP TABLE IF EXISTS q_impersonation SYNC")
    create = (
        "CREATE TABLE q_impersonation (line String) ENGINE = S3Queue("
        "gcs_impersonation_conn, format = 'LineAsString', "
        "impersonate_service_account = 'other@test-project.iam.gserviceaccount.com') "
        "SETTINGS mode = 'unordered', keeper_path = '/clickhouse/q_impersonation'"
    )

    assert "ACCESS_DENIED" in node.query_and_get_error(create)

    node.query(create, settings=allow)

    node.restart_clickhouse()

    assert "q_impersonation" in node.query("SHOW TABLES")

    node.query("DROP TABLE IF EXISTS q_impersonation SYNC")
