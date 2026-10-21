import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def drop_after_test():
    try:
        yield
    finally:
        node.query("DROP TABLE IF EXISTS tbl SYNC")
        node.exec_in_container(["bash", "-c", f"rm -fr /var/lib/clickhouse/shadow/"])


# Test that FREEZE operation can be cancelled with KILL QUERY.
def test_cancel_backup():
    if node.is_built_with_sanitizer():
        pytest.skip("Creating 20K parts under sanitizers can be slow.")

    # Freezing so much parts should take at least 2 seconds
    parts = 20_000
    node.query(
        f"CREATE TABLE tbl (x UInt32, y UInt32) ENGINE=MergeTree() PARTITION BY (x%{parts}) ORDER BY x"
    )
    node.query(
        f"INSERT INTO tbl SELECT number, number FROM numbers({parts}) SETTINGS max_partitions_per_insert_block={parts}"
    )

    uuid = node.query("SELECT uuid FROM system.tables WHERE name='tbl'").strip()
    shadow_path = f"/var/lib/clickhouse/shadow/test/store/{uuid[:3]}/{uuid}"

    freeze = node.get_query_request("ALTER TABLE tbl FREEZE WITH NAME 'test'")

    for _ in range(100):
        shadow_dir_exists = (
            len(
                node.exec_in_container(
                    ["bash", "-c", f"ls /var/lib/clickhouse/shadow/"], nothrow=True
                )
            )
            > 0
        )
        if (
            shadow_dir_exists
            and node.query(
                f"SELECT count() FROM system.processes WHERE query_kind == 'Alter' AND query LIKE '%FREEZE%'"
            )
            == "1\n"
        ):
            node.query(
                f"KILL QUERY WHERE query_kind == 'Alter' AND query LIKE '%FREEZE%' SYNC"
            )
            assert "killed in pending state" in freeze.get_error()

            assert parts > len(
                node.exec_in_container(["bash", "-c", f"ls {shadow_path}"])
                .strip()
                .split()
            )
            return

    raise RuntimeError("Query has finished before it could be killed")
