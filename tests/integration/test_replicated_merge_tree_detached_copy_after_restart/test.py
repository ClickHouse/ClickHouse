import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance("node1", with_zookeeper=True, stay_alive=True)
node2 = cluster.add_instance("node2", with_zookeeper=True)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def create_table(node, table):
    node.query(
        f"CREATE TABLE {table} (key UInt64, value String) "
        f"ENGINE = ReplicatedMergeTree('/clickhouse/tables/{table}', '{node.name}') "
        "ORDER BY key SETTINGS min_bytes_for_wide_part = 0"
    )


def drop_table(table):
    for node in (node1, node2):
        node.query(f"DROP TABLE IF EXISTS {table} SYNC")


def data_path(node, table):
    return node.query(
        f"SELECT data_paths[1] FROM system.tables WHERE database = 'default' AND name = '{table}'"
    ).strip()


def ls(node, path):
    return node.exec_in_container(["bash", "-c", f"ls {path}"], user="root").split()


def inode(node, path):
    return node.exec_in_container(["stat", "-c", "%i", path], user="root").strip()


def check_part(node, table):
    return node.query(
        f"CHECK TABLE {table} PART 'all_0_0_0' SETTINGS check_query_single_value_result = 0"
    )


def is_readonly(table):
    return f"SELECT is_readonly FROM system.replicas WHERE table = '{table}'"


def setup_broken_part(table):
    create_table(node1, table)
    create_table(node2, table)
    node1.query(
        f"INSERT INTO {table} SELECT number, toString(number) FROM numbers(1000)"
    )
    node2.query(f"SYSTEM SYNC REPLICA {table}")

    path = data_path(node1, table)
    node1.query(f"SYSTEM STOP CLEANUP {table}")
    node1.exec_in_container(
        [
            "bash",
            "-c",
            f"printf 'XXXXXXXX' | dd of={path}all_0_0_0/value.bin bs=1 seek=0 conv=notrunc",
        ],
        user="root",
    )
    check_part(node1, table)
    assert "broken_all_0_0_0" in ls(node1, path + "detached")
    assert "all_0_0_0" in ls(node1, path)
    return path


def test_broken_part_copy(started_cluster):
    table = "t_broken"
    try:
        path = setup_broken_part(table)

        node1.restart_clickhouse(kill=True)

        assert_eq_with_retry(node1, is_readonly(table), "0")
        assert_eq_with_retry(node1, f"SELECT count() FROM {table}", "1000")
        node1.wait_for_log_line(
            rf"default\.{table} .*Unexpected part all_0_0_0 has the same files as detached part broken_all_0_0_0"
        )
        detached = ls(node1, path + "detached")
        assert "broken_all_0_0_0" in detached
        assert "ignored_all_0_0_0" not in detached
        # all_0_0_0 is fetched again from node2, it is not the hard-linked broken copy.
        assert inode(node1, f"{path}all_0_0_0/key.bin") != inode(
            node1, f"{path}detached/broken_all_0_0_0/key.bin"
        )
        assert check_part(node1, table).split("\t")[1] == "1"
    finally:
        drop_table(table)


def test_incomplete_copy_is_kept(started_cluster):
    table = "t_incomplete"
    try:
        path = setup_broken_part(table)
        node1.exec_in_container(
            ["rm", f"{path}detached/broken_all_0_0_0/key.bin"], user="root"
        )

        node1.restart_clickhouse(kill=True)

        node1.wait_for_log_line(
            rf"local set of parts of table default\.{table} .*TOO_MANY_UNEXPECTED_DATA_PARTS",
            timeout=60,
        )
        assert node1.query(is_readonly(table)).strip() == "1"
        assert "all_0_0_0" in ls(node1, path)
    finally:
        drop_table(table)


def test_unexpected_part_copy(started_cluster):
    table = "t_unexpected"
    try:
        create_table(node1, table)
        node1.query(
            f"INSERT INTO {table} SELECT number, toString(number) FROM numbers(1000)"
        )
        path = data_path(node1, table)
        # The part check detaches a part that is not in Keeper only if it is older than 5 minutes.
        node1.exec_in_container(
            ["touch", "-d", "2 hours ago", f"{path}all_0_0_0"], user="root"
        )
        node1.restart_clickhouse(kill=True)
        assert_eq_with_retry(node1, is_readonly(table), "0")

        zk = cluster.get_kazoo_client("zoo1")
        zk.delete(
            f"/clickhouse/tables/{table}/replicas/node1/parts/all_0_0_0",
            recursive=True,
        )
        zk.stop()
        zk.close()

        node1.query(f"SYSTEM STOP CLEANUP {table}")
        check_part(node1, table)
        assert "unexpected_all_0_0_0" in ls(node1, path + "detached")
        assert "all_0_0_0" in ls(node1, path)

        node1.restart_clickhouse(kill=True)

        assert_eq_with_retry(node1, is_readonly(table), "0")
        assert "all_0_0_0" not in ls(node1, path)
        detached = ls(node1, path + "detached")
        assert "unexpected_all_0_0_0" in detached
        assert "ignored_all_0_0_0" not in detached
    finally:
        drop_table(table)


def test_cloned_replica_copy(started_cluster):
    table = "t_clone"
    try:
        create_table(node1, table)
        create_table(node2, table)
        node1.query(
            f"INSERT INTO {table} SELECT number, toString(number) FROM numbers(1000)"
        )
        node2.query(f"SYSTEM SYNC REPLICA {table}")
        path = data_path(node1, table)

        node1.query(f"SYSTEM STOP REPLICATION QUEUES {table}")
        node1.query(f"SYSTEM STOP CLEANUP {table}")
        node2.query(f"ALTER TABLE {table} DROP PART 'all_0_0_0'")

        zk = cluster.get_kazoo_client("zoo1")
        zk.set(f"/clickhouse/tables/{table}/replicas/node1/is_lost", b"1")
        zk.stop()
        zk.close()

        node1.query("SYSTEM RECONNECT ZOOKEEPER")
        node1.wait_for_log_line(rf"default\.{table} .*Will mimic node2", timeout=60)
        assert_eq_with_retry(
            node1,
            f"SELECT count() FROM system.detached_parts WHERE table = '{table}' AND name = 'clone_all_0_0_0'",
            "1",
        )
        assert "all_0_0_0" in ls(node1, path)

        node1.restart_clickhouse(kill=True)

        assert_eq_with_retry(node1, is_readonly(table), "0")
        assert "all_0_0_0" not in ls(node1, path)
        assert "clone_all_0_0_0" in ls(node1, path + "detached")
        assert node1.query(f"SELECT count() FROM {table}").strip() == "0"
    finally:
        drop_table(table)
