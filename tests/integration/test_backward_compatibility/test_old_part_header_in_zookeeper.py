import time

import pytest

from helpers.cluster import CLICKHOUSE_CI_MIN_TESTED_VERSION, ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    with_zookeeper=True,
    image="clickhouse/clickhouse-server",
    tag=CLICKHOUSE_CI_MIN_TESTED_VERSION,
    stay_alive=True,
    with_installed_binary=True,
)

ZK_PATH = "/clickhouse/tables/test_old_part_header/t"
# An old-format part node is empty and has `columns` and `checksums` children
OLD_FORMAT = (b"", ["checksums", "columns"])


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def cleanup():
    yield
    node.restart_with_original_version(clear_data_dir=True)


def part_nodes(zk, replica):
    path = f"{ZK_PATH}/replicas/{replica}/parts"
    return {
        name: (zk.get(f"{path}/{name}")[0], sorted(zk.get_children(f"{path}/{name}")))
        for name in zk.get_children(path)
    }


def create_replica(replica):
    node.query(
        f"""
        CREATE TABLE t_{replica} (k UInt64, v String)
        ENGINE = ReplicatedMergeTree('{ZK_PATH}', '{replica}')
        PARTITION BY k ORDER BY tuple()
        SETTINGS use_minimalistic_part_header_in_zookeeper = 0,
                 use_minimalistic_checksums_in_zookeeper = 0,
                 old_parts_lifetime = 1,
                 cleanup_delay_period = 0,
                 cleanup_delay_period_random_add = 0,
                 cleanup_thread_preferred_points_per_iteration = 0
        """
    )


def test_old_part_header_in_zookeeper(start_cluster):
    zk = cluster.get_kazoo_client("zoo1")
    if zk.exists(ZK_PATH):
        zk.delete(ZK_PATH, recursive=True)

    # The old version writes part nodes in the old format
    create_replica("r1")
    create_replica("r2")
    node.query("INSERT INTO t_r1 SELECT number % 3, toString(number) FROM numbers(30)")
    node.query("SYSTEM SYNC REPLICA t_r2", timeout=60)
    for replica in ("r1", "r2"):
        assert part_nodes(zk, replica) == {
            f"{k}_0_0_0": OLD_FORMAT for k in range(3)
        }, "the old version must write old-format part nodes"

    node.restart_with_latest_version()

    # They are still read, checked and fetched
    assert node.query("SELECT count() FROM t_r2") == "30\n"
    assert (
        node.query("CHECK TABLE t_r1 SETTINGS check_query_single_value_result = 1")
        == "1\n"
    )
    create_replica("r3")
    node.query("SYSTEM SYNC REPLICA t_r3", timeout=60)
    assert node.query("SELECT count() FROM t_r3") == "30\n"

    # A merge writes the compact header, and the old-format node of the merged part is removed
    node.query("OPTIMIZE TABLE t_r1 PARTITION 0 FINAL")
    node.query("SYSTEM SYNC REPLICA t_r2", timeout=60)
    node.query("SYSTEM SYNC REPLICA t_r3", timeout=60)
    for _ in range(120):
        if "0_0_0_0" not in part_nodes(zk, "r1"):
            break
        time.sleep(0.5)
    nodes = part_nodes(zk, "r1")
    assert "0_0_0_0" not in nodes
    assert nodes["0_0_0_1"][0].startswith(b"part header format version: 1\n")
    assert nodes["0_0_0_1"][1] == []
    assert nodes["1_0_0_0"] == OLD_FORMAT

    # A replica that still holds old-format part nodes is removed completely
    assert part_nodes(zk, "r2")["1_0_0_0"] == OLD_FORMAT
    node.query("DROP TABLE t_r2 SYNC")
    assert zk.exists(f"{ZK_PATH}/replicas/r2") is None
    node.query("DROP TABLE t_r1 SYNC")
    node.query("DROP TABLE t_r3 SYNC")
    assert zk.exists(ZK_PATH) is None
