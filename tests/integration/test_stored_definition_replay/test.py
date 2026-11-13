import shlex

import pytest

from helpers.cluster import ClickHouseCluster

# A definition stored by an older version may be one that a new definition (`CREATE` or a full-definition `ATTACH`)
# is refused for. It must still load, and whatever made it invalid must be handled when the object is used. Each
# test stores such a definition by editing the metadata file of a detached object and attaching it back with the
# short `ATTACH`, which replays the stored definition.

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/config.d/storage.xml", "configs/config.d/forbidden_headers.xml"],
    with_minio=True,
    # UNIQUE KEY reads and writes open a transaction, which needs Keeper with these features; S3Queue needs Keeper too.
    with_zookeeper=True,
    keeper_required_feature_flags=["filtered_list", "multi_read", "list_with_stat_and_data", "check_stat"],
    # The tests edit metadata files, which must be local.
    with_remote_database_disk=False,
)

# A second replica of a `Replicated` database, which must load the definitions the first one stored.
replica = cluster.add_instance("replica", with_zookeeper=True)

UK_SETTINGS = {"enable_unique_key": "1"}


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def edit_stored_table(table, sed_expression):
    metadata_sql = "/var/lib/clickhouse/" + node.query(
        f"SELECT metadata_path FROM system.tables WHERE database = 'default' AND name = '{table}'"
    ).strip()
    node.query(f"DETACH TABLE {table}")
    node.exec_in_container(["bash", "-c", f"sed -i {shlex.quote(sed_expression)} {shlex.quote(metadata_sql)}"])
    node.query(f"ATTACH TABLE {table}", settings={"send_logs_level": "error"})


def test_unique_key_projection_is_never_read():
    # A projection part bypasses the delete bitmap of a UNIQUE KEY table, so reads must use the base table.
    node.query("DROP TABLE IF EXISTS uk_proj SYNC")
    node.query(
        "CREATE TABLE uk_proj (id UInt64, user_id UInt32) ENGINE = MergeTree UNIQUE KEY (id) ORDER BY (id, user_id)",
        settings=UK_SETTINGS,
    )
    edit_stored_table(
        "uk_proj",
        r"s/^    `user_id` UInt32$/    `user_id` UInt32,\n    PROJECTION p (SELECT user_id, count() GROUP BY user_id)/",
    )
    assert node.query("SELECT name FROM system.projections WHERE database = 'default' AND table = 'uk_proj'") == "p\n"

    # Inserted after the ATTACH, so the parts carry the projection.
    node.query("INSERT INTO uk_proj VALUES (1, 10), (2, 10), (3, 20)", settings=UK_SETTINGS)
    assert node.query("SELECT id, user_id FROM uk_proj ORDER BY id") == "1\t10\n2\t10\n3\t20\n"
    assert (
        node.query(
            "SELECT user_id, count() FROM uk_proj GROUP BY user_id ORDER BY user_id",
            settings={"optimize_use_projections": 1},
        )
        == "10\t2\n20\t1\n"
    )
    assert "NOT_IMPLEMENTED" in node.query_and_get_error("SELECT * FROM mergeTreeProjection(currentDatabase(), uk_proj, p)")
    node.query("DROP TABLE uk_proj SYNC")


def test_unique_key_on_disk_without_append_refuses_insert():
    node.query("DROP TABLE IF EXISTS uk_no_append SYNC")
    node.query(
        "CREATE TABLE uk_no_append (c0 Int32) ENGINE = MergeTree ORDER BY c0 SETTINGS disk = 'plain_rewritable_without_append'"
    )
    edit_stored_table("uk_no_append", "s/^ORDER BY c0$/ORDER BY c0\\nUNIQUE KEY (c0)/")
    assert "SUPPORT_IS_DISABLED" in node.query_and_get_error("INSERT INTO uk_no_append VALUES (1)", settings=UK_SETTINGS)
    assert node.query("SELECT count() FROM uk_no_append", settings=UK_SETTINGS) == "0\n"
    node.query("DROP TABLE uk_no_append SYNC")


def test_s3queue_repeated_setting_uses_last_value():
    node.query("DROP TABLE IF EXISTS queue_repeated SYNC")
    node.query(
        "CREATE TABLE queue_repeated (a String) "
        f"ENGINE = S3Queue('http://{cluster.minio_host}:{cluster.minio_port}/{cluster.minio_bucket}/queue_repeated/*.csv', "
        "'minio', 'ClickHouse_Minio_P@ssw0rd', 'CSV') "
        "SETTINGS mode = 'unordered', keeper_path = '/clickhouse/queue_repeated', polling_min_timeout_ms = 1111"
    )
    edit_stored_table("queue_repeated", "s/polling_min_timeout_ms = 1111/polling_min_timeout_ms = 1111, polling_min_timeout_ms = 2222/")
    assert node.contains_in_log("has a duplicated setting polling_min_timeout_ms. Will use the last declared setting's value")
    assert (
        node.query(
            "SELECT value FROM system.s3_queue_settings WHERE database = 'default' AND table = 'queue_repeated' AND name = 'polling_min_timeout_ms'"
        )
        == "2222\n"
    )
    node.query("DROP TABLE queue_repeated SYNC")


def test_forbidden_auth_header_is_refused_on_first_use():
    # `exact_header` is forbidden by `http_forbid_headers`. The stored database attaches without contacting the
    # catalog, so it cannot block startup, and the header is refused when the catalog is first built.
    node.query("DROP DATABASE IF EXISTS catalog_forbidden SYNC")
    definition = (
        "ENGINE = DataLakeCatalog('http://localhost:18181/v1') "
        "SETTINGS catalog_type = 'rest', auth_header = 'exact_header: some_value', warehouse = 'demo'"
    )
    assert "is forbidden" in node.query_and_get_error(
        f"ATTACH DATABASE catalog_forbidden {definition}", settings={"allow_database_iceberg": 1}
    )
    node.exec_in_container(
        ["bash", "-c", f"echo {shlex.quote('ATTACH DATABASE _ ' + definition)} > /var/lib/clickhouse/metadata/catalog_forbidden.sql"]
    )
    node.query("ATTACH DATABASE catalog_forbidden", settings={"send_logs_level": "error"})
    assert node.query("SELECT count() FROM system.databases WHERE name = 'catalog_forbidden'") == "1\n"
    assert "is forbidden" in node.query_and_get_error("CHECK DATABASE catalog_forbidden")
    node.query("DROP DATABASE catalog_forbidden SYNC")


def test_new_replica_loads_refreshable_view_whose_target_was_replaced():
    # A refreshable view without APPEND in a Replicated database needs a replicated target, but the target can be
    # replaced after the view exists. A replica that joins later still has to load the view.
    database = "replay_refreshable"
    for instance in (node, replica):
        instance.query(f"DROP DATABASE IF EXISTS {database} SYNC")
    path = f"/clickhouse/databases/{database}"
    node.query(f"CREATE DATABASE {database} ENGINE = Replicated('{path}', 'shard1', 'replica1')")
    node.query(
        f"CREATE TABLE {database}.target_data (id Int64) ENGINE = MergeTree ORDER BY id",
        settings={"distributed_ddl_output_mode": "none"},
    )
    node.query(
        f"CREATE TABLE {database}.target (id Int64) ENGINE = ReplicatedMergeTree ORDER BY id",
        settings={"distributed_ddl_output_mode": "none"},
    )
    node.query(
        f"CREATE MATERIALIZED VIEW {database}.view REFRESH EVERY 100 YEAR TO {database}.target (id Int64) "
        f"AS SELECT id FROM {database}.target_data",
        settings={"distributed_ddl_output_mode": "none"},
    )
    node.query(f"DROP TABLE {database}.target SYNC", settings={"distributed_ddl_output_mode": "none"})
    node.query(f"RENAME TABLE {database}.target_data TO {database}.target", settings={"distributed_ddl_output_mode": "none"})

    replica.query(f"CREATE DATABASE {database} ENGINE = Replicated('{path}', 'shard1', 'replica2')")
    replica.query(f"SYSTEM SYNC DATABASE REPLICA {database}")
    assert replica.query(f"SELECT name FROM system.tables WHERE database = '{database}' ORDER BY name") == "target\nview\n"

    for instance in (node, replica):
        instance.query(f"DROP DATABASE {database} SYNC")
