import os

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.mock_servers import start_mock_servers

METADATA_SERVER_HOSTNAME = "resolver"
METADATA_SERVER_PORT = 8080

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    with_minio=True,
    main_configs=["configs/use_environment_credentials.xml"],
    env_variables={
        "AWS_EC2_METADATA_SERVICE_ENDPOINT": f"{METADATA_SERVER_HOSTNAME}:{METADATA_SERVER_PORT}",
    },
)


def start_metadata_server():
    script_dir = os.path.join(os.path.dirname(__file__), "metadata_servers")
    start_mock_servers(
        cluster,
        script_dir,
        [
            (
                "simple_server.py",
                METADATA_SERVER_HOSTNAME,
                METADATA_SERVER_PORT,
            )
        ],
    )


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        start_metadata_server()
        yield
    finally:
        cluster.shutdown()


def test_credentials_from_metadata():
    node.query(
        f"INSERT INTO FUNCTION s3('http://{cluster.minio_host}:{cluster.minio_port}/{cluster.minio_bucket}/test1.jsonl') SELECT * FROM numbers(100) SETTINGS s3_truncate_on_insert = 1"
    )

    assert (
        "100"
        == node.query(
            f"SELECT count() FROM s3('http://{cluster.minio_host}:{cluster.minio_port}/{cluster.minio_bucket}/test1.jsonl')"
        ).strip()
    )

    expected_logs = [
        "Calling EC2MetadataService to get token failed, falling back to a less secure way",
        "Getting default credentials for ec2 instance from resolver:8080",
        "Calling EC2MetadataService resource, /latest/meta-data/iam/security-credentials returned credential string myrole",
        "Calling EC2MetadataService resource /latest/meta-data/iam/security-credentials/myrole",
        "Successfully pulled credentials from EC2MetadataService with access key",
    ]

    node.query("SYSTEM FLUSH LOGS")
    for expected_msg in expected_logs:
        assert node.contains_in_log(
            "AWSEC2InstanceProfileConfigLoader: " + expected_msg
        )


def test_credentials_provider_caching():
    node.query(
        f"INSERT INTO FUNCTION s3('http://{cluster.minio_host}:{cluster.minio_port}/{cluster.minio_bucket}/test_caching.jsonl') SELECT * FROM numbers(10) SETTINGS s3_truncate_on_insert = 1"
    )

    for _ in range(10):
        result = node.query(
            f"SELECT count() FROM s3('http://{cluster.minio_host}:{cluster.minio_port}/{cluster.minio_bucket}/test_caching.jsonl')"
        ).strip()
        assert result == "10"

    added = int(
        node.query(
            "SELECT value FROM system.events WHERE event = 'S3CachedCredentialsProvidersAdded'"
        ).strip() or "0"
    )
    reused = int(
        node.query(
            "SELECT value FROM system.events WHERE event = 'S3CachedCredentialsProvidersReused'"
        ).strip() or "0"
    )

    assert added > 0
    assert reused > 0
    assert reused >= added

    current_cached = int(
        node.query(
            "SELECT value FROM system.metrics WHERE name = 'S3CachedCredentialsProviders'"
        ).strip() or "0"
    )
    assert current_cached > 0
