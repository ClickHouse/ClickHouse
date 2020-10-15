import time
import uuid

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    'node1',
    main_configs=['configs/remote_servers.xml'],
    with_zookeeper=True)

node2 = cluster.add_instance(
    'node2',
    main_configs=['configs/remote_servers.xml'],
    with_zookeeper=True)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_part_uuid(started_cluster):
    test_uuid = str(uuid.uuid4())

    for ix, n in enumerate([node1, node2]):
        n.query("""
        CREATE TABLE t(key UInt64, value UInt64)
        ENGINE ReplicatedMergeTree('/clickhouse/tables/t', '{}')
        ORDER BY tuple()
        """.format(ix))

    node1.query("INSERT INTO t VALUES (1, 1)")

    node1.query("ALTER TABLE t DETACH PARTITION tuple()")
    node1.exec_in_container([
        "bash", "-c",
        "echo '{}' > /var/lib/clickhouse/data/default/t/detached/{}/uuid.txt".format(test_uuid, "all_0_0_0")
    ])
    node1.query("ALTER TABLE t ATTACH PARTITION tuple()")
    node1.query("ALTER TABLE t UPDATE value = 1 WHERE key = 1")

    assert test_uuid == node1.query("SELECT uuid FROM system.parts WHERE table = 't' AND active ORDER BY name").strip()

    node2.query("SYSTEM SYNC REPLICA t")
    assert test_uuid == node2.query("SELECT uuid FROM system.parts WHERE table = 't' AND active ORDER BY name").strip()
