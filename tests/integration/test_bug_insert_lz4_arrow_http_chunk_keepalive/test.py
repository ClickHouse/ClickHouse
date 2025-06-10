import os
import docker
from helpers.cluster import run_and_check
from docker.errors import ContainerError

import pytest

from helpers.cluster import ClickHouseCluster, QueryRuntimeException

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def get_network_name(docker_client, container_name):
    container = docker_client.containers.get(
        cluster.get_instance_docker_id(container_name)
    )
    networks = container.attrs["NetworkSettings"]["Networks"]
    network_names = list(networks.keys())
    assert len(network_names) > 0, "Container {} has no networks".format(container_name)
    return network_names[0]


def test_insert_all_rows(start_cluster):
    docker_client = docker.DockerClient(
        base_url="unix:///var/run/docker.sock",
        version=cluster.docker_api_version,
        timeout=600,
    )

    current_test_data_dir = os.path.join(os.path.dirname(__file__), "data")
    try:
        docker_client.containers.run(
            "gradle:8.5-jdk21",
            "/app/run.sh",
            volumes=[f"{current_test_data_dir}:/app"],
            working_dir="/app",
            network=get_network_name(docker_client, "node1"),
            remove=True,
            environment={
                "CLICKHOUSE_HOST": "node1",
            },
        )
    except ContainerError as e:
        # Container exited with non-zero code
        print(f"Container failed with exit code: {e.exit_status}")
        raise
