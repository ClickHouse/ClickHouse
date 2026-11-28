import time

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

standalone = cluster.add_instance(
    "standalone",
    main_configs=["configs/metadata_change_log.xml"],
)

replica1 = cluster.add_instance(
    "replica1",
    with_zookeeper=True,
    stay_alive=True,
    main_configs=["configs/metadata_change_log.xml"],
)

replica2 = cluster.add_instance(
    "replica2",
    with_zookeeper=True,
    stay_alive=True,
    main_configs=["configs/metadata_change_log.xml"],
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def set_snapshot_token(node, token):
    token_xml = f"<snapshot_token>{token}</snapshot_token>" if token else ""
    node.replace_config(
        "/etc/clickhouse-server/config.d/metadata_change_log.xml",
        f"""<clickhouse>
    <metadata_change_log>
        <database>system</database>
        <table>metadata_change_log</table>
        <partition_by>toYYYYMM(event_date)</partition_by>
        <flush_interval_milliseconds>500</flush_interval_milliseconds>
        {token_xml}
    </metadata_change_log>
</clickhouse>""",
    )
    node.query("SYSTEM RELOAD CONFIG")


def wait_for_snapshot_rows(nodes, snapshot_id, expected_min=1, timeout=30):
    deadline = time.time() + timeout
    while time.time() < deadline:
        total = 0
        for n in nodes:
            n.query("SYSTEM FLUSH LOGS metadata_change_log")
            total += int(
                n.query(
                    f"SELECT count() FROM system.metadata_change_log WHERE snapshot_id = '{snapshot_id}'"
                ).strip()
            )
        if total >= expected_min:
            return total
        time.sleep(0.5)
    raise AssertionError(f"Timed out waiting for snapshot_id={snapshot_id} rows")


def test_standalone_hot_reload_snapshot(started_cluster):
    standalone.query("DROP DATABASE IF EXISTS snap_db")
    standalone.query("CREATE DATABASE snap_db ENGINE = Atomic COMMENT 'snap comment'")
    standalone.query(
        """
        CREATE TABLE snap_db.orders (
            id UInt64 COMMENT 'pk',
            amount Float64 DEFAULT 0
        ) ENGINE = MergeTree
        ORDER BY id
        COMMENT 'orders table'
        """
    )
    standalone.query(
        """
        CREATE TABLE snap_db.orders_dst (id UInt64) ENGINE = MergeTree ORDER BY id
        """
    )
    standalone.query(
        """
        CREATE MATERIALIZED VIEW snap_db.orders_mv TO snap_db.orders_dst AS
        SELECT id FROM snap_db.orders
        """
    )

    set_snapshot_token(standalone, "snap-1")
    wait_for_snapshot_rows([standalone], "snap-1", expected_min=4)

    db_row = standalone.query(
        """
        SELECT event_type, database, engine, comment, startsWith(create_query, 'CREATE DATABASE')
        FROM system.metadata_change_log
        WHERE snapshot_id = 'snap-1' AND database = 'snap_db' AND event_type = 'DatabaseSnapshot'
        """
    ).strip()
    assert db_row == "DatabaseSnapshot\tsnap_db\tAtomic\tsnap comment\t1"

    tbl_row = standalone.query(
        """
        SELECT event_type, table, engine, sorting_key, comment, columns_name, columns_type, columns_comment
        FROM system.metadata_change_log
        WHERE snapshot_id = 'snap-1' AND database = 'snap_db' AND table = 'orders' AND event_type = 'TableSnapshot'
        """
    ).strip()
    assert (
        tbl_row
        == "TableSnapshot\torders\tMergeTree\tid\torders table\t['id','amount']\t['UInt64','Float64']\t['pk','']"
    )

    mv_deps = standalone.query(
        """
        SELECT dependencies_database, dependencies_table
        FROM system.metadata_change_log
        WHERE snapshot_id = 'snap-1' AND database = 'snap_db' AND table = 'orders_mv' AND event_type = 'TableSnapshot'
        """
    ).strip()
    assert mv_deps == "['snap_db','snap_db']\t['orders','orders_dst']"

    internal_count = int(
        standalone.query(
            """
            SELECT count()
            FROM system.metadata_change_log
            WHERE snapshot_id = 'snap-1'
              AND database IN ('system', 'INFORMATION_SCHEMA', 'information_schema', '_temporary_and_external_tables')
            """
        ).strip()
    )
    assert internal_count == 0

    # Re-reloading the same token is a no-op.
    before_count = int(
        standalone.query(
            "SELECT count() FROM system.metadata_change_log WHERE snapshot_id = 'snap-1'"
        ).strip()
    )
    standalone.query("SYSTEM RELOAD CONFIG")
    time.sleep(1)
    standalone.query("SYSTEM FLUSH LOGS metadata_change_log")
    after_count = int(
        standalone.query(
            "SELECT count() FROM system.metadata_change_log WHERE snapshot_id = 'snap-1'"
        ).strip()
    )
    assert after_count == before_count

    # Bumping the token triggers a fresh snapshot.
    set_snapshot_token(standalone, "snap-2")
    wait_for_snapshot_rows([standalone], "snap-2", expected_min=4)


def test_keeper_deduplication_and_restart_idempotency(started_cluster):
    for node in (replica1, replica2):
        node.query("DROP DATABASE IF EXISTS zk_snap_db")
        node.query("CREATE DATABASE zk_snap_db ENGINE = Atomic")
        node.query(
            "CREATE TABLE zk_snap_db.items (id UInt64) ENGINE = MergeTree ORDER BY id"
        )

    set_snapshot_token(replica1, "zk-snap-1")
    set_snapshot_token(replica2, "zk-snap-1")

    wait_for_snapshot_rows([replica1, replica2], "zk-snap-1", expected_min=2)
    # Give both replicas time to settle and confirm only one emitted.
    time.sleep(2)

    r1_count = int(
        replica1.query(
            "SELECT count() FROM system.metadata_change_log WHERE snapshot_id = 'zk-snap-1' AND database = 'zk_snap_db'"
        ).strip()
    )
    r2_count = int(
        replica2.query(
            "SELECT count() FROM system.metadata_change_log WHERE snapshot_id = 'zk-snap-1' AND database = 'zk_snap_db'"
        ).strip()
    )

    assert sorted([r1_count, r2_count]) == [0, 2]

    zk = cluster.get_kazoo_client("zoo1")
    completed_data, _ = zk.get("/clickhouse/metadata_change_log/snapshots/completed_tokens")
    assert completed_data.decode("utf-8").splitlines() == ["zk-snap-1"]
    assert zk.get_children("/clickhouse/metadata_change_log/snapshots") == ["completed_tokens"]

    # Restarting the node that emitted (or the peer) must not re-emit zk-snap-1.
    winner = replica1 if r1_count == 2 else replica2
    winner.restart_clickhouse()
    time.sleep(1)
    winner.query("SYSTEM FLUSH LOGS metadata_change_log")
    post_restart_count = int(
        winner.query(
            "SELECT count() FROM system.metadata_change_log WHERE snapshot_id = 'zk-snap-1' AND database = 'zk_snap_db'"
        ).strip()
    )
    assert post_restart_count == 2

    # A second token reuses the same single completed_tokens znode (no unbounded per-token znodes).
    set_snapshot_token(replica1, "zk-snap-2")
    set_snapshot_token(replica2, "zk-snap-2")
    wait_for_snapshot_rows([replica1, replica2], "zk-snap-2", expected_min=2)
    time.sleep(1)
    completed_data, _ = zk.get("/clickhouse/metadata_change_log/snapshots/completed_tokens")
    assert completed_data.decode("utf-8").splitlines() == ["zk-snap-1", "zk-snap-2"]
    assert zk.get_children("/clickhouse/metadata_change_log/snapshots") == ["completed_tokens"]
