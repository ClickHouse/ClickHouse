import logging
import os
import time

import pytest

import helpers.s3_url_proxy_tests_util as proxy_util
from helpers.cluster import ClickHouseCluster
from helpers.config_cluster import minio_secret_key

# Credentials the proxy in auth-proxy/auth_proxy.py demands. The password is
# percent-encoded in the environment variable so that the decoding path is covered.
PROXY_USER = "user"
PROXY_PASSWORD_ENCODED = "p%40ssword"
PROXY_HOST = "resolver"
PROXY_PORT = 8081


def run_auth_proxy(cluster, current_dir):
    container_id = cluster.get_container_id("resolver")
    cluster.copy_file_to_container(
        container_id,
        os.path.join(current_dir, "auth-proxy", "auth_proxy.py"),
        "auth_proxy.py",
    )
    cluster.exec_in_container(container_id, ["python", "auth_proxy.py"], detach=True)

    # The image is not guaranteed to have bash, so probe the port with python.
    probe = (
        "import socket,sys;"
        "s=socket.socket();"
        "s.settimeout(1);"
        f"sys.exit(s.connect_ex(('127.0.0.1',{PROXY_PORT})))"
    )
    for attempt in range(10):
        response = cluster.exec_in_container(
            container_id, ["python", "-c", probe], nothrow=True
        )
        if response is not None and "Traceback" not in str(response):
            return
        time.sleep(attempt)

    assert False, "Auth proxy did not start"


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)

        # Disable `with_remote_database_disk` as the test uses a proxy, which might not
        # work with the default configs of the remote database disk.
        # This instance deliberately does not set `instance_env_variables`, so its
        # variables land in the shared cluster env file alongside MINIO_*. The other
        # two take a private copy of that env and override `http_proxy`.
        cluster.add_instance(
            "env_node_with_credentials",
            with_minio=True,
            env_variables={
                "http_proxy": f"http://{PROXY_USER}:{PROXY_PASSWORD_ENCODED}@{PROXY_HOST}:{PROXY_PORT}",
            },
            with_remote_database_disk=False,
        )

        cluster.add_instance(
            "env_node_wrong_credentials",
            with_minio=True,
            env_variables={
                "http_proxy": f"http://{PROXY_USER}:wrong@{PROXY_HOST}:{PROXY_PORT}",
            },
            instance_env_variables=True,
            with_remote_database_disk=False,
        )

        cluster.add_instance(
            "env_node_without_credentials",
            with_minio=True,
            env_variables={
                "http_proxy": f"http://{PROXY_HOST}:{PROXY_PORT}",
            },
            instance_env_variables=True,
            with_remote_database_disk=False,
        )

        # Credentials sent to a proxy that does not ask for them must be harmless:
        # proxy1 is the stock nginx proxy used by the other proxy tests.
        cluster.add_instance(
            "env_node_credentials_unused",
            with_minio=True,
            env_variables={
                "http_proxy": f"http://{PROXY_USER}:{PROXY_PASSWORD_ENCODED}@proxy1",
            },
            instance_env_variables=True,
            with_remote_database_disk=False,
        )

        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        run_auth_proxy(cluster, os.path.dirname(__file__))
        logging.info("Auth proxy started")

        yield cluster
    finally:
        cluster.shutdown()


def test_s3_with_proxy_credentials(cluster):
    # The proxy rejects anything without a valid Proxy-Authorization header, so these
    # queries only pass if the credentials from http_proxy reached it.
    minio_endpoint = proxy_util.build_s3_endpoint("http", "env_node_with_credentials")
    proxy_util.remove_existing_s3_endpoint(cluster.minio_client, minio_endpoint)

    node = cluster.instances["env_node_with_credentials"]
    proxy_util.perform_simple_queries(node, minio_endpoint)

    logs = cluster.get_container_logs("resolver")
    assert "ALLOWED" in logs, "Proxy never accepted an authenticated request"


def test_s3_without_proxy_credentials_is_rejected(cluster):
    # Same proxy, no credentials in http_proxy: the request must be refused, otherwise
    # the test above would prove nothing.
    minio_endpoint = proxy_util.build_s3_endpoint(
        "http", "env_node_without_credentials"
    )
    node = cluster.instances["env_node_without_credentials"]

    # Cap the retries: a 407 is currently treated as retryable, so without this the
    # query would retry for a long time before surfacing the error.
    with pytest.raises(Exception) as exception:
        node.query(
            f"SELECT * FROM s3('{minio_endpoint}', 'minio', '{minio_secret_key}', 'CSV') "
            "SETTINGS s3_max_single_read_retries = 1, s3_retry_attempts = 1, "
            "s3_request_timeout_ms = 5000"
        )

    assert "HTTP response code: 407" in str(exception.value)


def test_s3_with_wrong_proxy_credentials_is_rejected(cluster):
    # Credentials are sent but do not match, so the proxy must still refuse.
    minio_endpoint = proxy_util.build_s3_endpoint("http", "env_node_wrong_credentials")
    node = cluster.instances["env_node_wrong_credentials"]

    with pytest.raises(Exception) as exception:
        node.query(
            f"SELECT * FROM s3('{minio_endpoint}', 'minio', '{minio_secret_key}', 'CSV') "
            "SETTINGS s3_max_single_read_retries = 1, s3_retry_attempts = 1, "
            "s3_request_timeout_ms = 5000"
        )

    assert "HTTP response code: 407" in str(exception.value)


def test_s3_credentials_against_proxy_that_does_not_require_them(cluster):
    # No-regression case: the proxy ignores the Proxy-Authorization header, so the
    # queries must behave exactly as they do without credentials.
    proxy_util.simple_test(cluster, ["proxy1"], "http", "env_node_credentials_unused")
