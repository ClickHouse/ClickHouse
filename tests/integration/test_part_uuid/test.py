import uuid

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    'node1',
    main_configs=['configs/remote_servers.xml', 'configs/merge_tree_uuids.xml'],
    with_zookeeper=True)

node2 = cluster.add_instance(
    'node2',
    main_configs=['configs/remote_servers.xml', 'configs/merge_tree_uuids.xml', 'configs/merge_tree_in_memory.xml'],
    with_zookeeper=True)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_part_uuid(started_cluster):
    uuid_zero = uuid.UUID(bytes=b"\x00" * 16)

    for ix, n in enumerate([node1, node2]):
        n.query("""
        CREATE TABLE t(key UInt64, value UInt64)
        ENGINE ReplicatedMergeTree('/clickhouse/tables/t', '{}')
        ORDER BY tuple()
        """.format(ix))

    # Test insert assigns uuid to part.
    node1.query("INSERT INTO t VALUES (1, 1)")

    uuids = set()
    for node in [node1, node2]:
        node.query("SYSTEM SYNC REPLICA t")
        part_initial_uuid = uuid.UUID(node.query("SELECT uuid FROM system.parts WHERE table = 't' AND active ORDER BY name").strip())
        uuids.add(part_initial_uuid)
        assert uuid_zero != part_initial_uuid
    assert len(uuids) == 1, "expect the same uuid on all the replicas"

    # Test detach / attach.
    node1.query("ALTER TABLE t DETACH PARTITION tuple(); ALTER TABLE t ATTACH PARTITION tuple()")

    for node in [node1, node2]:
        node.query("SYSTEM SYNC REPLICA t")
        part_reattach_uuid = uuid.UUID(node.query(
            "SELECT uuid FROM system.parts WHERE table = 't' AND active ORDER BY name").strip())
        assert part_initial_uuid == part_reattach_uuid

    # Test mutation assigns new non-zero uuids.
    node1.query("ALTER TABLE t UPDATE value = 1 WHERE key = 1 SETTINGS mutations_sync = 2")
    part_mutate_uuid = uuid.UUID(node1.query("SELECT uuid FROM system.parts WHERE table = 't' AND active ORDER BY name").strip())
    assert part_mutate_uuid not in [uuid_zero, part_initial_uuid]

    node2.query("SYSTEM SYNC REPLICA t")
    assert part_mutate_uuid == uuid.UUID(node2.query(
        "SELECT uuid FROM system.parts WHERE table = 't' AND active ORDER BY name").strip())

    # Test merge assigns new non-zero uuids.
    node2.query("INSERT INTO t VALUES (1, 1)")
    node2.query("OPTIMIZE TABLE t FINAL")

    uuids = set()
    for node in [node1, node2]:
        node.query("SYSTEM SYNC REPLICA t")
        part_merge_uuid = uuid.UUID(node.query(
            "SELECT uuid FROM system.parts WHERE table = 't' AND active ORDER BY name").strip())
        uuids.add(part_merge_uuid)
        assert part_mutate_uuid not in [uuid_zero, part_merge_uuid]
    assert len(uuids) == 1, "expect the same uuid on all the replicas"


def test_part_uuid_wal(started_cluster):
    uuid_zero = uuid.UUID(bytes=b"\x00" * 16)

    for ix, n in enumerate([node1, node2]):
        n.query("""
        CREATE TABLE t_wal(key UInt64, value UInt64)
        ENGINE ReplicatedMergeTree('/clickhouse/tables/t_wal', '{}')
        ORDER BY tuple()
        """.format(ix))

    node2.query("INSERT INTO t_wal VALUES (1, 1)")

    uuids = set()
    for node in [node1, node2]:
        node.query("SYSTEM SYNC REPLICA t")
        part_initial_uuid = uuid.UUID(node.query("SELECT uuid FROM system.parts WHERE table = 't_wal' AND active ORDER BY name").strip())
        assert "InMemory" == node.query("SELECT part_type FROM system.parts WHERE table = 't_wal' AND active ORDER BY name").strip()
        uuids.add(part_initial_uuid)
        assert uuid_zero != part_initial_uuid
    assert len(uuids) == 1, "expect the same uuid on all the replicas"

    # Test detach / attach table to trigger WAL processing.
    for node in [node1, node2]:
        node.query("DETACH TABLE t_wal; ATTACH TABLE t_wal")
        part_reattach_uuid = uuid.UUID(node.query(
            "SELECT uuid FROM system.parts WHERE table = 't_wal' AND active ORDER BY name").strip())
        assert part_initial_uuid == part_reattach_uuid
