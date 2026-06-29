"""
Test ClickHouse Keeper compatibility with the Java ZooKeeper client.

Specifically tests all watch-related request types that require correct
response serialization:
  - addWatch (OpCode 106): PERSISTENT and PERSISTENT_RECURSIVE modes
  - checkWatches (OpCode 17): verify watch existence
  - removeWatches (OpCode 18): remove all watches of a type
  - setWatches (OpCode 101): re-establish watches (data/exist/child)
  - setWatches2 (OpCode 105): re-establish watches including persistent

See https://github.com/ClickHouse/ClickHouse/issues/98079
"""

import os

import pytest

from helpers import keeper_utils
from helpers.cluster import ClickHouseCluster, run_and_check

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/keeper_config.xml"],
    stay_alive=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        keeper_utils.wait_until_connected(cluster, node)
        yield cluster
    finally:
        cluster.shutdown()


def run_java_test(test_name):
    """Copy JAR to container and run a specific Java test."""
    instance_id = cluster.get_instance_docker_id("node")
    instance_ip = cluster.get_instance_ip("node")
    jar_path = os.path.join(SCRIPT_DIR, "java_client", "keeper-java-client-test.jar")

    run_and_check(
        [
            "docker cp {local} {cont_id}:{dist}".format(
                local=jar_path,
                cont_id=instance_id,
                dist="/tmp/keeper-java-client-test.jar",
            )
        ],
        shell=True,
    )

    result = run_and_check(
        [
            'docker exec {cont_id} bash -lc "java -jar /tmp/keeper-java-client-test.jar {ip}:9181 {test}"'.format(
                cont_id=instance_id,
                ip=instance_ip,
                test=test_name,
            )
        ],
        shell=True,
    )

    print(result)
    assert "FAIL" not in result, f"Java test '{test_name}' had failures:\n{result}"
    assert "Results:" in result, f"Java test '{test_name}' did not produce results:\n{result}"


def test_addwatch(started_cluster):
    """Test addWatch with PERSISTENT and PERSISTENT_RECURSIVE modes.

    This is the primary regression test for issue #98079: the Java ZooKeeper
    client expects an ErrorResponse body (4-byte error code) in the addWatch
    response. Without it, the client gets EOFException and disconnects.
    """
    run_java_test("addWatch")


def test_check_watches(started_cluster):
    """Test checkWatches (OpCode 17): verify a watch exists on a path.

    The server sends no body for this response (just ReplyHeader).
    """
    run_java_test("checkWatches")


def test_remove_watches(started_cluster):
    """Test removeWatches (OpCode 18): remove all watches of a given type.

    The server sends no body for this response (just ReplyHeader).
    """
    run_java_test("removeWatches")


def test_set_watches(started_cluster):
    """Test setWatches (OpCode 101): data/exist/child watch lists.

    The server sends no body for this response (just ReplyHeader).
    """
    run_java_test("setWatches")


def test_set_watches2(started_cluster):
    """Test setWatches2 (OpCode 105): watches including persistent watches.

    The server sends no body for this response (just ReplyHeader).
    """
    run_java_test("setWatches2")
