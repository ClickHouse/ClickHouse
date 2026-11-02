import json
import os

import pytest

from helpers.cluster import ClickHouseCluster

CONFIG_PATH = "/tmp/keeper_bench.yaml"
ITERATIONS = 100

cluster = ClickHouseCluster(__file__)
# use_keeper=False makes the ensemble real Apache ZooKeeper instead of ClickHouse Keeper,
# which is the point of this test: Keeper accepts its own private op codes, ZooKeeper does not.
node = cluster.add_instance("node", with_zookeeper=True, use_keeper=False)


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_keeper_bench_runs_against_zookeeper(started_cluster):
    node.copy_file_to_container(
        os.path.join(os.path.dirname(__file__), "configs", "keeper_bench.yaml"),
        CONFIG_PATH,
    )

    output = node.exec_in_container(
        ["bash", "-c", f"clickhouse keeper-bench --config {CONFIG_PATH} 2>&1"],
        user="root",
        nothrow=True,
    )

    # `mainEntryClickHouseKeeperBench` catches every exception and returns 0, so the exit
    # status carries no signal and the run has to be judged by its output.
    assert "Got exception while trying to run benchmark" not in output, output

    reports = [line for line in output.splitlines() if line.startswith('{"timestamp"')]
    assert reports, output
    report = json.loads(reports[-1])
    assert report["errors"] == 0, output
    assert report["ops"] == ITERATIONS, output
