import pytest

from helpers.cluster import ClickHouseCluster


@pytest.fixture(scope="module")
def started_cluster():
    global cluster
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "node1",
            main_configs=["config.xml"],
            with_zookeeper=True,
            with_minio=True,
            env_variables={'HOME': '/tmp'}, # for $HOME/.disks-file-history
        )
        cluster.start()

        yield cluster

    finally:
        cluster.shutdown()

def simple_test(node, disk):
    node.exec_in_container(
        [
            "bash",
            "-c",
            f"echo 'meow' | /usr/bin/clickhouse disks --disk {disk} --query 'write im_a_file.txt'",
        ]
    )
    out = node.exec_in_container(
        [
            "/usr/bin/clickhouse",
            "disks",
            "--disk",
            disk,
            "--query",
            "read im_a_file.txt",
        ]
    )
    assert out == "meow\n\n"

def test_disks_app_plain_rewritable(started_cluster):
    simple_test(cluster.instances["node1"], "plainRewritableDisk")
