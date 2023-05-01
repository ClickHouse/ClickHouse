import time

import pytest
from helpers.client import CommandRequest
from helpers.client import QueryTimeoutExceedException
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/conf.d/merge_tree.xml", "configs/conf.d/remote_servers.xml"],
    with_zookeeper=True,
    macros={"layer": 0, "shard": 0, "replica": 1},
)
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/conf.d/merge_tree.xml", "configs/conf.d/remote_servers.xml"],
    with_zookeeper=True,
    macros={"layer": 0, "shard": 0, "replica": 2},
)
nodes = [node1, node2]


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        pass
        cluster.shutdown()


def test_deduplication_window_in_seconds(started_cluster):
    node = node1

    node1.query(
        """
        CREATE TABLE simple ON CLUSTER test_cluster (date Date, id UInt32)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/simple', '{replica}') PARTITION BY toYYYYMM(date) ORDER BY id"""
    )

    node.query("INSERT INTO simple VALUES (0, 0)")
    time.sleep(1)
    node.query("INSERT INTO simple VALUES (0, 0)")  # deduplication works here
    node.query("INSERT INTO simple VALUES (0, 1)")
    assert TSV(node.query("SELECT count() FROM simple")) == TSV("2\n")

    # wait clean thread
    time.sleep(2)

    assert (
        TSV.toMat(
            node.query(
                "SELECT count() FROM system.zookeeper WHERE path='/clickhouse/tables/0/simple/blocks'"
            )
        )[0][0]
        == "1"
    )
    node.query(
        "INSERT INTO simple VALUES (0, 0)"
    )  # deduplication doesn't works here, the first hash node was deleted
    assert TSV.toMat(node.query("SELECT count() FROM simple"))[0][0] == "3"

    node1.query("""DROP TABLE simple ON CLUSTER test_cluster""")


# Currently this test just reproduce incorrect behavior that sould be fixed
@pytest.mark.skip(reason="Flapping test")
def test_deduplication_works_in_case_of_intensive_inserts(started_cluster):
    inserters = []
    fetchers = []

    node1.query(
        """
        CREATE TABLE simple ON CLUSTER test_cluster (date Date, id UInt32)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/simple', '{replica}') PARTITION BY toYYYYMM(date) ORDER BY id"""
    )

    node1.query("INSERT INTO simple VALUES (0, 0)")

    for node in nodes:
        host = node.ip_address

        inserters.append(
            CommandRequest(
                ["/bin/bash"],
                timeout=10,
                stdin="""
set -e
for i in `seq 1000`; do
    {} --host {} -q "INSERT INTO simple VALUES (0, 0)"
done
""".format(
                    cluster.get_client_cmd(), host
                ),
            )
        )

        fetchers.append(
            CommandRequest(
                ["/bin/bash"],
                timeout=10,
                stdin="""
set -e
for i in `seq 1000`; do
    res=`{} --host {} -q "SELECT count() FROM simple"`
    if [[ $? -ne 0 || $res -ne 1 ]]; then
        echo "Selected $res elements! Host: {}" 1>&2
        exit -1
    fi;
done
""".format(
                    cluster.get_client_cmd(), host, node.name
                ),
            )
        )

    # There were not errors during INSERTs
    for inserter in inserters:
        try:
            inserter.get_answer()
        except QueryTimeoutExceedException:
            # Only timeout is accepted
            pass

    # There were not errors during SELECTs
    for fetcher in fetchers:
        try:
            fetcher.get_answer()
        except QueryTimeoutExceedException:
            # Only timeout is accepted
            pass

    node1.query("""DROP TABLE simple ON CLUSTER test_cluster""")
