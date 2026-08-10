"""Tests for the `lazy_load_tables` database setting.

Every test runs over each engine pair in `lazy_env.build_environments`, so the same bodies cover
`Atomic`/`MergeTree` and `Atomic`/`ReplicatedMergeTree` here and can cover a `Shared`/`SharedMergeTree`
pair in a private build.
"""

import json

import pytest

import helpers.kafka.common as k
from helpers.cluster import ClickHouseCluster
from helpers.config_cluster import nats_pass, nats_user

from .lazy_env import BACKUP, MOVE_PARTITION, REPLICATION, build_environments, ENVIRONMENT_IDS

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "instance",
    main_configs=["configs/lazy.xml"],
    with_kafka=True,
    with_nats=True,
    with_rabbitmq=True,
    with_zookeeper=True,
    stay_alive=True,
)

DB = "lazy"


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(params=ENVIRONMENT_IDS)
def env(started_cluster, request):
    environment = build_environments(instance)[request.param]
    environment.create_database(DB)
    yield environment
    instance.query(f"DROP DATABASE IF EXISTS {DB} SYNC")


def test_mergetree_family_is_deferred(env):
    """The MergeTree family is what the setting is for, so it must still be deferred."""
    env.create_table(DB, "t", "id UInt64")
    env.reload(DB)
    assert env.is_deferred(DB, "t")


def test_other_engines_stay_eager(env):
    """Engines reached by casting to a concrete storage type must not be deferred."""
    instance.query(
        f"""
        CREATE TABLE {DB}.st (id UInt64) ENGINE = Set;
        CREATE TABLE {DB}.jn (k UInt64, v String) ENGINE = Join(ANY, LEFT, k);
        CREATE TABLE {DB}.mem (id UInt64) ENGINE = Memory;
        CREATE TABLE {DB}.lg (id UInt64) ENGINE = Log;
        CREATE TABLE {DB}.tlg (id UInt64) ENGINE = TinyLog;
        CREATE TABLE {DB}.slg (id UInt64) ENGINE = StripeLog;
        CREATE TABLE {DB}.nul (id UInt64) ENGINE = Null;
        CREATE TABLE {DB}.km (k String, v String) ENGINE = KeeperMap('/lazy_km') PRIMARY KEY k;
        """
    )
    env.create_table(DB, "src", "id UInt64")
    instance.query(
        f"""
        CREATE TABLE {DB}.mrg (id UInt64) ENGINE = Merge('{DB}', 'src');
        CREATE TABLE {DB}.buf (id UInt64) ENGINE = Buffer('{DB}', 'src', 1, 100, 100, 10, 100, 10000, 1000000);
        CREATE TABLE {DB}.dist (id UInt64) ENGINE = Distributed('single_shard_cluster', '{DB}', 'src');
        """
    )

    env.reload(DB)

    for table in ["st", "jn", "mem", "lg", "tlg", "slg", "nul", "km", "mrg", "buf", "dist"]:
        assert not env.is_deferred(DB, table), f"{table} must stay eager"
    assert env.is_deferred(DB, "src")


def test_streaming_engines_stay_eager(env):
    """Message queues start consuming in startup, which a proxy would defer indefinitely."""
    instance.query(
        f"""
        CREATE TABLE {DB}.kafka_t (v String) ENGINE = Kafka
        SETTINGS kafka_broker_list = 'kafka1:19092', kafka_topic_list = 'unused_kafka',
                 kafka_group_name = 'unused_kafka_group', kafka_format = 'JSONEachRow';

        CREATE TABLE {DB}.nats_t (v String) ENGINE = NATS
        SETTINGS nats_url = 'nats1:4444', nats_subjects = 'unused_subject', nats_format = 'JSONEachRow',
                 nats_username = '{nats_user}', nats_password = '{nats_pass}';

        CREATE TABLE {DB}.rabbit_t (v String) ENGINE = RabbitMQ
        SETTINGS rabbitmq_host_port = 'rabbitmq1:5672', rabbitmq_exchange_name = 'unused_exchange',
                 rabbitmq_format = 'JSONEachRow';
        """
    )

    env.reload(DB)

    for table in ["kafka_t", "nats_t", "rabbit_t"]:
        assert not env.is_deferred(DB, table), f"{table} must stay eager"


def test_kafka_ingests_after_restart(env):
    """A Kafka table feeding a view must ingest after a restart with nobody reading it."""
    topic = f"lazy_topic_{env.table_engine}"
    admin_client = k.get_admin_client(cluster)
    k.kafka_create_topic(admin_client, topic)
    try:
        instance.query(
            f"""
            CREATE TABLE {DB}.kafka_src (key UInt64, value UInt64) ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092', kafka_topic_list = '{topic}',
                     kafka_group_name = '{topic}_group', kafka_format = 'JSONEachRow';
            """
        )
        env.create_table(DB, "dest", "key UInt64, value UInt64", order_by="key")
        instance.query(
            f"CREATE MATERIALIZED VIEW {DB}.mv TO {DB}.dest AS SELECT key, value FROM {DB}.kafka_src"
        )

        instance.restart_clickhouse()

        assert not env.is_deferred(DB, "kafka_src")

        messages = [json.dumps({"key": i, "value": i * 2}) for i in range(50)]
        k.kafka_produce(cluster, topic, messages)

        # Nothing reads kafka_src: only the engine's own stream can move these rows.
        instance.query_with_retry(
            f"SELECT count() FROM {DB}.dest",
            check_callback=lambda result: int(result) == 50,
            retry_count=60,
            sleep_time=1,
        )
        assert int(instance.query(f"SELECT sum(value) FROM {DB}.dest")) == sum(
            i * 2 for i in range(50)
        )
    finally:
        k.kafka_delete_topic(admin_client, topic)


def test_metadata_visible_before_first_access(env):
    """A deferred table must report its real structure without being loaded to answer."""
    env.create_table(
        DB,
        "t",
        "d Date, id UInt64, v String, n UInt32, "
        "INDEX idx_v v TYPE bloom_filter GRANULARITY 1, "
        "PROJECTION proj (SELECT n, count() GROUP BY n)",
        order_by="(id, v)",
        extra="PARTITION BY toYYYYMM(d) PRIMARY KEY id SAMPLE BY id TTL d + INTERVAL 10 YEAR",
    )
    instance.query(
        f"INSERT INTO {DB}.t SELECT '2024-01-01', number, toString(number), number % 5 FROM numbers(10)"
    )
    env.reload(DB)

    assert env.is_deferred(DB, "t")

    row = instance.query(
        f"SELECT sorting_key, partition_key, primary_key, sampling_key "
        f"FROM system.tables WHERE database = '{DB}' AND name = 't'"
    ).strip()
    assert row == "id, v\ttoYYYYMM(d)\tid\tid", row

    assert (
        instance.query(
            f"SELECT name FROM system.data_skipping_indices WHERE database = '{DB}' AND table = 't'"
        ).strip()
        == "idx_v"
    )
    assert (
        instance.query(
            f"SELECT name FROM system.projections WHERE database = '{DB}' AND table = 't'"
        ).strip()
        == "proj"
    )
    assert (
        instance.query(
            f"SELECT groupArray(name) FROM (SELECT name FROM system.columns "
            f"WHERE database = '{DB}' AND table = 't' AND is_in_sorting_key ORDER BY name)"
        ).strip()
        == "['id','v']"
    )

    # Reading the metadata must not have forced the real storage to be created.
    assert env.is_deferred(DB, "t")


def test_versioned_collapsing_sorting_key(env):
    """VersionedCollapsingMergeTree appends its version column to the sorting key, so a deferred
    table must report the same key as a loaded one."""
    instance.query(
        f"CREATE TABLE {DB}.vcmt (id UInt64, sign Int8, ver UInt64) "
        f"ENGINE = VersionedCollapsingMergeTree(sign, ver) ORDER BY id"
    )
    env.reload(DB)

    assert env.is_deferred(DB, "vcmt")
    deferred = instance.query(
        f"SELECT sorting_key FROM system.tables WHERE database = '{DB}' AND name = 'vcmt'"
    ).strip()

    assert int(instance.query(f"SELECT count() FROM {DB}.vcmt")) == 0
    loaded = instance.query(
        f"SELECT sorting_key FROM system.tables WHERE database = '{DB}' AND name = 'vcmt'"
    ).strip()

    assert deferred == loaded == "id, ver", f"deferred={deferred!r} loaded={loaded!r}"


def test_mutations(env):
    """`checkMutationIsPossible` is asked before `mutate`, so it has to reach the real storage."""
    env.create_table(
        DB,
        "t",
        "id UInt64, v String, n UInt32, "
        "INDEX idx_v v TYPE bloom_filter GRANULARITY 1, "
        "PROJECTION proj (SELECT n, count() GROUP BY n)",
    )
    instance.query(
        f"INSERT INTO {DB}.t SELECT number, toString(number), number % 5 FROM numbers(10)"
    )

    env.reload(DB)
    instance.query(f"ALTER TABLE {DB}.t DELETE WHERE id = 0 SETTINGS mutations_sync = 2")
    assert int(instance.query(f"SELECT count() FROM {DB}.t")) == 9

    env.reload(DB)
    instance.query(f"DELETE FROM {DB}.t WHERE id = 1")
    assert int(instance.query(f"SELECT count() FROM {DB}.t")) == 8

    env.reload(DB)
    instance.query(f"ALTER TABLE {DB}.t MATERIALIZE INDEX idx_v SETTINGS mutations_sync = 2")

    env.reload(DB)
    instance.query(f"ALTER TABLE {DB}.t MATERIALIZE PROJECTION proj SETTINGS mutations_sync = 2")

    env.reload(DB)
    instance.query(f"ALTER TABLE {DB}.t UPDATE v = 'x' WHERE id = 2 SETTINGS mutations_sync = 2")
    assert instance.query(f"SELECT v FROM {DB}.t WHERE id = 2").strip() == "x"


def test_lightweight_update(env):
    """`supportsLightweightUpdate` is consulted on the catalog pointer before `updateLightweight`."""
    env.create_table(
        DB,
        "t",
        "id UInt64, v String",
        extra="SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1",
    )
    instance.query(f"INSERT INTO {DB}.t SELECT number, toString(number) FROM numbers(10)")
    env.reload(DB)

    assert env.is_deferred(DB, "t")
    instance.query(f"UPDATE {DB}.t SET v = 'updated' WHERE id = 3")
    assert instance.query(f"SELECT v FROM {DB}.t WHERE id = 3").strip() == "updated"


def test_deprecated_engine_syntax_is_eager(env):
    """The deprecated engine-argument syntax keeps its keys in the engine arguments, which the proxy
    cannot report, so such a table must not be deferred."""
    if env.table_engine != "MergeTree":
        pytest.skip("the deprecated syntax test is specific to plain MergeTree")

    instance.query(
        f"CREATE TABLE {DB}.legacy (d Date, id UInt64, v String) ENGINE = MergeTree(d, id, 8192)",
        settings={"allow_deprecated_syntax_for_merge_tree": 1},
    )
    env.reload(DB)

    assert not env.is_deferred(DB, "legacy")
    assert (
        instance.query(
            f"SELECT sorting_key, partition_key FROM system.tables "
            f"WHERE database = '{DB}' AND name = 'legacy'"
        ).strip()
        == "id\ttoYYYYMM(d)"
    )


def test_comment_visible_before_first_access(env):
    """`system.tables.comment` is read from the metadata, so the proxy has to carry it."""
    env.create_table(DB, "t", "id UInt64", extra="COMMENT 'a lazy table'")
    env.reload(DB)

    assert env.is_deferred(DB, "t")
    assert (
        instance.query(
            f"SELECT comment FROM system.tables WHERE database = '{DB}' AND name = 't'"
        ).strip()
        == "a lazy table"
    )


def test_system_schedule_merge(env):
    """SYSTEM SCHEDULE MERGE reaches the storage through a MergeTreeData cast."""
    env.create_table(
        DB, "t", "id UInt64", extra="SETTINGS merge_selector_algorithm = 'Manual'"
    )
    instance.query(f"INSERT INTO {DB}.t SELECT number FROM numbers(10)")
    instance.query(f"INSERT INTO {DB}.t SELECT number + 10 FROM numbers(10)")
    parts = instance.query(
        f"SELECT name FROM system.parts WHERE database = '{DB}' AND table = 't' AND active ORDER BY name"
    ).split()
    assert len(parts) == 2, parts
    env.reload(DB)

    assert env.is_deferred(DB, "t")
    part_list = ", ".join(f"'{part}'" for part in parts)
    instance.query(f"SYSTEM SCHEDULE MERGE {DB}.t PARTS {part_list}")


def test_system_load_primary_key(env):
    """SYSTEM LOAD/UNLOAD PRIMARY KEY reaches the storage through a MergeTreeData cast."""
    env.create_table(DB, "t", "id UInt64")
    instance.query(f"INSERT INTO {DB}.t SELECT number FROM numbers(10)")
    env.reload(DB)

    assert env.is_deferred(DB, "t")
    instance.query(f"SYSTEM UNLOAD PRIMARY KEY {DB}.t")
    instance.query(f"SYSTEM LOAD PRIMARY KEY {DB}.t")


def test_alter_modify_ttl(env):
    """`supportsTTL` gates ALTER MODIFY TTL and defaults to false on the proxy."""
    env.create_table(DB, "t", "d Date, id UInt64")
    env.reload(DB)
    instance.query(f"ALTER TABLE {DB}.t MODIFY TTL d + INTERVAL 10 YEAR")
    assert "TTL" in instance.query(f"SHOW CREATE TABLE {DB}.t")


def test_backup_and_restore(env):
    """`IStorage::backupData` is a no-op, so an unforwarded proxy backs up nothing."""
    env.require(BACKUP)
    env.create_table(DB, "t", "id UInt64, v String")
    instance.query(f"INSERT INTO {DB}.t SELECT number, toString(number) FROM numbers(1000)")
    env.reload(DB)

    assert env.is_deferred(DB, "t")
    backup = f"lazy_backup_{env.table_engine}"
    instance.query(f"BACKUP TABLE {DB}.t TO Disk('backups', '{backup}')")

    # Restoring under a new name would reuse the replicated table's coordination path.
    instance.query(f"DROP TABLE {DB}.t SYNC")
    instance.query(f"RESTORE TABLE {DB}.t FROM Disk('backups', '{backup}')")

    assert int(instance.query(f"SELECT count() FROM {DB}.t")) == 1000
    assert int(instance.query(f"SELECT sum(id) FROM {DB}.t")) == sum(range(1000))


def test_backup_database(env):
    """A whole-database backup collects each table through the same `backupData` call."""
    env.require(BACKUP)
    env.create_table(DB, "a", "d Date, id UInt64", extra="PARTITION BY toYYYYMM(d)")
    env.create_table(DB, "b", "id UInt64")
    instance.query(f"INSERT INTO {DB}.a SELECT '2024-01-01', number FROM numbers(500)")
    instance.query(f"INSERT INTO {DB}.b SELECT number FROM numbers(700)")
    env.reload(DB)

    assert env.is_deferred(DB, "a") and env.is_deferred(DB, "b")
    backup = f"lazy_db_backup_{env.table_engine}"
    instance.query(f"BACKUP DATABASE {DB} TO Disk('backups', '{backup}')")

    # Restoring under a new name would reuse the replicated tables' coordination paths.
    instance.query(f"DROP DATABASE {DB} SYNC")
    instance.query(f"RESTORE DATABASE {DB} FROM Disk('backups', '{backup}')")

    assert int(instance.query(f"SELECT count() FROM {DB}.a")) == 500
    assert int(instance.query(f"SELECT count() FROM {DB}.b")) == 700


def test_backup_partitions(env):
    """Partition-level backup also needs `supportsBackupPartition` from the real storage."""
    env.require(BACKUP)
    env.create_table(DB, "t", "d Date, id UInt64", extra="PARTITION BY toYYYYMM(d)")
    instance.query(f"INSERT INTO {DB}.t SELECT '2024-01-01', number FROM numbers(500)")
    instance.query(f"INSERT INTO {DB}.t SELECT '2024-02-01', number FROM numbers(300)")
    env.reload(DB)

    assert env.is_deferred(DB, "t")
    backup = f"lazy_part_backup_{env.table_engine}"
    instance.query(f"BACKUP TABLE {DB}.t PARTITIONS '202401' TO Disk('backups', '{backup}')")
    instance.query(f"DROP TABLE {DB}.t SYNC")
    instance.query(f"RESTORE TABLE {DB}.t FROM Disk('backups', '{backup}')")

    assert int(instance.query(f"SELECT count() FROM {DB}.t")) == 500


def test_incremental_backup(env):
    """An incremental backup diffs against the base backup's contents, which must not be empty."""
    env.require(BACKUP)
    env.create_table(DB, "t", "id UInt64")
    instance.query(f"INSERT INTO {DB}.t SELECT number FROM numbers(100)")
    env.reload(DB)

    base = f"lazy_base_{env.table_engine}"
    instance.query(f"BACKUP TABLE {DB}.t TO Disk('backups', '{base}')")

    instance.query(f"INSERT INTO {DB}.t SELECT number + 100 FROM numbers(50)")
    env.reload(DB)

    incremental = f"lazy_incr_{env.table_engine}"
    instance.query(
        f"BACKUP TABLE {DB}.t TO Disk('backups', '{incremental}') "
        f"SETTINGS base_backup = Disk('backups', '{base}')"
    )
    instance.query(f"DROP TABLE {DB}.t SYNC")
    instance.query(f"RESTORE TABLE {DB}.t FROM Disk('backups', '{incremental}')")

    assert int(instance.query(f"SELECT count() FROM {DB}.t")) == 150


def test_async_insert_table_setting(env):
    """The table-level `async_insert` setting is read through `areAsynchronousInsertsEnabled`."""
    env.create_table(DB, "t", "id UInt64", extra="SETTINGS async_insert = 1")
    env.reload(DB)

    assert env.is_deferred(DB, "t")
    query_id = f"lazy_async_insert_{env.table_engine}"
    instance.query(
        f"INSERT INTO {DB}.t SETTINGS async_insert = 0 VALUES (1)", query_id=query_id
    )
    instance.query("SYSTEM FLUSH LOGS asynchronous_insert_log")
    assert (
        int(
            instance.query(
                f"SELECT count() FROM system.asynchronous_insert_log WHERE query_id = '{query_id}'"
            )
        )
        == 1
    )


def test_trivial_count_and_parallel_replicas(env):
    """The planner asks the storage whether it is a MergeTree and whether trivial count applies."""
    env.create_table(DB, "t", "id UInt64, v UInt64")
    instance.query(f"INSERT INTO {DB}.t SELECT number, number * 2 FROM numbers(1000)")

    env.reload(DB)
    plan = instance.query(f"EXPLAIN SELECT count() FROM {DB}.t")
    assert "Optimized trivial count" in plan, plan

    env.reload(DB)
    settings = (
        "enable_parallel_replicas = 1, max_parallel_replicas = 3, "
        "cluster_for_parallel_replicas = 'parallel_replicas_cluster', "
        "parallel_replicas_for_non_replicated_merge_tree = 1, "
        "parallel_replicas_min_number_of_rows_per_replica = 0"
    )
    plan = instance.query(f"EXPLAIN SELECT sum(v) FROM {DB}.t SETTINGS {settings}")
    assert "ParallelReplicas" in plan, plan


def test_system_parts_after_access(env):
    """`system.parts` reaches storages by casting to MergeTreeData, which a proxy is not."""
    env.create_table(DB, "t", "id UInt64")
    instance.query(f"INSERT INTO {DB}.t SELECT number FROM numbers(10)")
    env.reload(DB)

    # Before any access the table is not loaded, so it has no parts to report.
    assert env.is_deferred(DB, "t")
    assert int(instance.query(f"SELECT count() FROM system.parts WHERE database = '{DB}'")) == 0

    # After the first access the real storage exists and must be visible.
    assert int(instance.query(f"SELECT count() FROM {DB}.t")) == 10
    assert int(instance.query(f"SELECT count() FROM system.parts WHERE database = '{DB}' AND active")) == 1
    assert int(instance.query(f"SELECT sum(rows) FROM system.parts WHERE database = '{DB}' AND active")) == 10
    assert int(instance.query(f"SELECT total_rows FROM system.tables WHERE database = '{DB}' AND name = 't'")) == 10


def test_system_mutations_after_access(env):
    """`system.mutations` casts to MergeTreeData as well."""
    env.create_table(DB, "t", "id UInt64")
    instance.query(f"INSERT INTO {DB}.t SELECT number FROM numbers(10)")
    env.reload(DB)
    instance.query(f"ALTER TABLE {DB}.t DELETE WHERE id = 0 SETTINGS mutations_sync = 2")
    assert int(instance.query(f"SELECT count() FROM system.mutations WHERE database = '{DB}'")) >= 1


def test_system_replicas_after_access(env):
    """`system.replicas` casts to StorageReplicatedMergeTree."""
    env.require(REPLICATION)
    env.create_table(DB, "t", "id UInt64")
    instance.query(f"INSERT INTO {DB}.t SELECT number FROM numbers(10)")
    env.reload(DB)

    assert int(instance.query(f"SELECT count() FROM {DB}.t")) == 10
    assert int(instance.query(f"SELECT count() FROM system.replicas WHERE database = '{DB}'")) == 1


def test_system_sync_replica(env):
    """SYSTEM SYNC REPLICA names the table, so loading it is expected."""
    env.require(REPLICATION)
    env.create_table(DB, "t", "id UInt64")
    env.reload(DB)
    assert env.is_deferred(DB, "t")
    instance.query(f"SYSTEM SYNC REPLICA {DB}.t")


def test_move_partition_to_table(env):
    """Both sides of MOVE PARTITION are reached by casting to MergeTreeData."""
    env.require(MOVE_PARTITION)
    env.create_table(DB, "src", "d Date, id UInt64", extra="PARTITION BY toYYYYMM(d)")
    env.create_table(DB, "dst", "d Date, id UInt64", extra="PARTITION BY toYYYYMM(d)")
    instance.query(f"INSERT INTO {DB}.src VALUES ('2024-01-01', 1)")
    env.reload(DB)

    assert env.is_deferred(DB, "src") and env.is_deferred(DB, "dst")
    instance.query(f"ALTER TABLE {DB}.src MOVE PARTITION 202401 TO TABLE {DB}.dst")
    assert int(instance.query(f"SELECT count() FROM {DB}.dst")) == 1
    assert int(instance.query(f"SELECT count() FROM {DB}.src")) == 0


def test_attach_partition_from(env):
    """The source of ATTACH PARTITION FROM is reached by casting to MergeTreeData."""
    env.require(MOVE_PARTITION)
    env.create_table(DB, "src", "d Date, id UInt64", extra="PARTITION BY toYYYYMM(d)")
    env.create_table(DB, "dst", "d Date, id UInt64", extra="PARTITION BY toYYYYMM(d)")
    instance.query(f"INSERT INTO {DB}.src VALUES ('2024-01-01', 1)")
    env.reload(DB)

    assert env.is_deferred(DB, "src") and env.is_deferred(DB, "dst")
    instance.query(f"ALTER TABLE {DB}.dst ATTACH PARTITION 202401 FROM {DB}.src")
    assert int(instance.query(f"SELECT count() FROM {DB}.dst")) == 1


def test_replicas_status_endpoint(env):
    """The /replicas_status handler casts to StorageReplicatedMergeTree."""
    env.require(REPLICATION)
    env.create_table(DB, "t", "id UInt64")
    env.reload(DB)
    assert int(instance.query(f"SELECT count() FROM {DB}.t")) == 0
    assert "Ok" in instance.http_request("replicas_status", method="GET").text
