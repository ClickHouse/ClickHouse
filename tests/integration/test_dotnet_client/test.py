# coding: utf-8

import os

import docker
import pytest

from helpers.cluster import ClickHouseCluster, get_docker_compose_path, run_and_check

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
DOCKER_COMPOSE_PATH = get_docker_compose_path()

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    user_configs=["configs/users.xml"],
)


@pytest.fixture(scope="module")
def started_cluster():
    cluster.start()
    try:
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(scope="module")
def dotnet_container():
    docker_compose = os.path.join(
        DOCKER_COMPOSE_PATH, "docker_compose_dotnet_client.yml"
    )
    run_and_check(
        cluster.compose_cmd(
            "-f",
            docker_compose,
            "up",
            "--force-recreate",
            "-d",
            "--no-build",
        )
    )
    yield docker.from_env().containers.get(cluster.get_instance_docker_id("dotnet1"))


def test_dotnet_client(started_cluster, dotnet_container):
    with open(os.path.join(SCRIPT_DIR, "dotnet.reference"), "rb") as fp:
        reference = fp.read()

    code, (stdout, stderr) = dotnet_container.exec_run(
        "dotnet /client/out/clickhouse.test.dll --host {host} --port {port} --user default --password 123 --database default".format(
            host=started_cluster.get_instance_ip("node"), port=8123
        ),
        demux=True,
    )

    # These two streams exist nowhere else: the client runs in its own container and nothing in the
    # job collects its output. Reporting them here is what makes a client that died on a signal
    # diagnosable at all, instead of leaving a bare exit code behind.
    assert code == 0, f"stdout:\n{stdout!r}\nstderr:\n{stderr!r}"
    assert stdout == reference, f"stderr:\n{stderr!r}"
