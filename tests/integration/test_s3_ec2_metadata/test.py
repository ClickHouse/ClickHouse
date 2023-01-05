import pytest
from helpers.cluster import ClickHouseCluster
import logging
import os
import time

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

EC2_METADATA_SERVER_HOSTNAME = "resolver"
EC2_METADATA_SERVER_PORT = 8080

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    with_minio=True,
    main_configs=["configs/use_environment_credentials.xml"],
    env_variables={
        "AWS_EC2_METADATA_SERVICE_ENDPOINT": f"{EC2_METADATA_SERVER_HOSTNAME}:{EC2_METADATA_SERVER_PORT}",
    },
)


def start_ec2_metadata_server():
    logging.info("Starting EC2 metadata server")
    container_id = cluster.get_container_id("resolver")

    cluster.copy_file_to_container(
        container_id,
        os.path.join(SCRIPT_DIR, "ec2_metadata_server/request_response_server.py"),
        "request_response_server.py",
    )

    cluster.exec_in_container(
        container_id,
        ["python", "request_response_server.py", str(EC2_METADATA_SERVER_PORT)],
        detach=True,
    )

    # Wait for the server to start.
    num_attempts = 100
    for attempt in range(num_attempts):
        ping_response = cluster.exec_in_container(
            container_id,
            ["curl", "-s", f"http://localhost:{EC2_METADATA_SERVER_PORT}/"],
            nothrow=True,
        )
        if ping_response != "OK":
            if attempt == num_attempts - 1:
                assert ping_response == "OK", 'Expected "OK", but got "{}"'.format(
                    ping_response
                )
            else:
                time.sleep(1)
        else:
            logging.debug(
                f"request_response_server.py answered {ping_response} on attempt {attempt}"
            )
            break

    logging.info("EC2 metadata server started")


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        start_ec2_metadata_server()
        yield
    finally:
        cluster.shutdown()


def test_credentials_from_ec2_metadata():
    node.query(
        f"INSERT INTO FUNCTION s3('http://{cluster.minio_host}:{cluster.minio_port}/{cluster.minio_bucket}/test1.jsonl') SELECT * FROM numbers(100)"
    )

    assert (
        "100"
        == node.query(
            f"SELECT count() FROM s3('http://{cluster.minio_host}:{cluster.minio_port}/{cluster.minio_bucket}/test1.jsonl')"
        ).strip()
    )

    expected_logs = [
        "Getting default credentials for ec2 instance from resolver:8080",
        "Calling EC2MetadataService resource, /latest/meta-data/iam/security-credentials returned credential string myrole",
        "Calling EC2MetadataService resource /latest/meta-data/iam/security-credentials/myrole",
        "Successfully pulled credentials from EC2MetadataService with access key",
    ]

    for expected_msg in expected_logs:
        node.contains_in_log("AWSEC2InstanceProfileConfigLoader: " + expected_msg)
