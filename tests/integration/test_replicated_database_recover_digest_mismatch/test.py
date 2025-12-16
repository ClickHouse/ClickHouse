import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry
from helpers.database_disk import get_database_disk_name

cluster = ClickHouseCluster(__file__)

main_node = cluster.add_instance(
    "main_node",
    main_configs=["configs/config.xml"],
    user_configs=["configs/settings.xml"],
    with_zookeeper=True,
    stay_alive=True,
    with_remote_database_disk=False,
    macros={"shard": 1, "replica": 1},
)
dummy_node = cluster.add_instance(
    "dummy_node",
    main_configs=["configs/config.xml"],
    user_configs=["configs/settings2.xml"],
    with_zookeeper=True,
    stay_alive=True,
    with_remote_database_disk=False,
    macros={"shard": 1, "replica": 2},
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def create_some_tables(db):
    settings = {
        "distributed_ddl_task_timeout": 0,
        "allow_suspicious_codecs": 1,
    }
    main_node.query(f"CREATE TABLE {db}.t1 (n int) ENGINE=Memory", settings=settings)
    dummy_node.query(
        f"CREATE TABLE {db}.t2 (s String) ENGINE=Memory", settings=settings
    )
    main_node.query(
        f"CREATE TABLE {db}.mt1 (n int) ENGINE=MergeTree order by n",
        settings=settings,
    )
    dummy_node.query(
        f"CREATE TABLE {db}.mt2 (n int) ENGINE=MergeTree order by n",
        settings=settings,
    )
    main_node.query(
        f"CREATE TABLE {db}.rmt1 (n int) ENGINE=ReplicatedMergeTree order by n",
        settings=settings,
    )
    dummy_node.query(
        f"CREATE TABLE {db}.rmt2 (n int CODEC(ZSTD, ZSTD, ZSTD(12), LZ4HC(12))) ENGINE=ReplicatedMergeTree order by n",
        settings=settings,
    )
    main_node.query(
        f"CREATE TABLE {db}.rmt3 (n int, json JSON materialized '{{}}') ENGINE=ReplicatedMergeTree order by n",
        settings=settings,
    )
    dummy_node.query(
        f"CREATE TABLE {db}.rmt5 (n int) ENGINE=ReplicatedMergeTree order by n",
        settings=settings,
    )
    main_node.query(
        f"CREATE MATERIALIZED VIEW {db}.mv1 (n int) ENGINE=ReplicatedMergeTree order by n AS SELECT n FROM {db}.rmt1",
        settings=settings,
    )
    dummy_node.query(
        f"CREATE MATERIALIZED VIEW {db}.mv2 (n int) ENGINE=ReplicatedMergeTree order by n  AS SELECT n FROM {db}.rmt2",
        settings=settings,
    )
    main_node.query(
        f"CREATE DICTIONARY {db}.d1 (n int DEFAULT 0, m int DEFAULT 1) PRIMARY KEY n "
        "SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 USER 'default' TABLE 'rmt1' PASSWORD '' DB 'recover')) "
        "LIFETIME(MIN 1 MAX 10) LAYOUT(FLAT())"
    )
    dummy_node.query(
        f"CREATE DICTIONARY {db}.d2 (n int DEFAULT 0, m int DEFAULT 1) PRIMARY KEY n "
        "SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 USER 'default' TABLE 'rmt2' PASSWORD '' DB 'recover')) "
        "LIFETIME(MIN 1 MAX 10) LAYOUT(FLAT())"
    )


def test_recover_digest_mismatch(started_cluster):
    main_node.query("DROP DATABASE IF EXISTS recover_digest_mismatch SYNC")
    dummy_node.query("DROP DATABASE IF EXISTS recover_digest_mismatch SYNC")

    main_node.query(
        "CREATE DATABASE recover_digest_mismatch ENGINE = Replicated('/clickhouse/databases/recover_digest_mismatch', 'shard1', 'replica1');"
    )
    dummy_node.query(
        "CREATE DATABASE recover_digest_mismatch ENGINE = Replicated('/clickhouse/databases/recover_digest_mismatch', 'shard1', 'replica2');"
    )

    create_some_tables("recover_digest_mismatch")

    main_node.query("SYSTEM SYNC DATABASE REPLICA recover_digest_mismatch")
    dummy_node.query("SYSTEM SYNC DATABASE REPLICA recover_digest_mismatch")

    db_disk_name = get_database_disk_name(dummy_node)
    db_data_path = dummy_node.query(
        f"SELECT metadata_path FROM system.databases WHERE database='recover_digest_mismatch'"
    ).strip()

    disk_cmd_prefix = f"/usr/bin/clickhouse disks -C /etc/clickhouse-server/config.xml --disk {db_disk_name} --save-logs --query "
    db_disk_path = dummy_node.query(
        "SELECT path FROM system.disks WHERE name='{db_disk_name}'"
    ).strip()

    print(f"db_data_path {db_data_path}")

    mv1_metadata = dummy_node.exec_in_container(
        ["bash", "-c", f"{disk_cmd_prefix} 'read --path-from {db_data_path}mv1.sql' "]
    )
    corrupted_mv1_metadata = (
        mv1_metadata.replace("Int32", "String").replace("`", r"\`").replace('"', r"\"")
    )
    ways_to_corrupt_metadata = [
        f"{disk_cmd_prefix} 'move --path-from {db_data_path}t1.sql --path-to {db_data_path}m1.sql'",
        f"""printf "%s" "{corrupted_mv1_metadata}" | {disk_cmd_prefix} 'write --path-to {db_data_path}mv1.sql'""",
        f"{disk_cmd_prefix} 'remove {db_data_path}d1.sql'",
        "rm -rf /var/lib/clickhouse/metadata/recover_digest_mismatch/",  # Will trigger "Directory already exists"
        f"{disk_cmd_prefix} 'remove -r {db_disk_path}store/' && rm -rf /var/lib/clickhouse/store",  # Remove both metadata and data
    ]

    for command in ways_to_corrupt_metadata:
        print(f"Corrupting data using `{command}`")
        need_remove_is_active_node = "rm -rf" in command
        dummy_node.stop_clickhouse(kill=not need_remove_is_active_node)
        dummy_node.exec_in_container(["bash", "-c", command])

        query = (
            "SELECT name, uuid, create_table_query FROM system.tables WHERE database='recover_digest_mismatch' AND name NOT LIKE '.inner_id.%' "
            "ORDER BY name SETTINGS show_table_uuid_in_table_create_query_if_not_nil=1"
        )
        expected = main_node.query(query)

        if need_remove_is_active_node:
            # NOTE Otherwise it fails to recreate ReplicatedMergeTree table due to "Replica already exists"
            main_node.query(
                "SYSTEM DROP REPLICA '2' FROM DATABASE recover_digest_mismatch"
            )

        # There is a race condition between deleting active node and creating it on server startup
        # So we start a server only after we deleted all table replicas from the Keeper
        dummy_node.start_clickhouse()
        assert_eq_with_retry(dummy_node, query, expected)

    main_node.query("DROP DATABASE IF EXISTS recover_digest_mismatch SYNC")
    dummy_node.query("DROP DATABASE IF EXISTS recover_digest_mismatch SYNC")

    print("Everything Okay")
