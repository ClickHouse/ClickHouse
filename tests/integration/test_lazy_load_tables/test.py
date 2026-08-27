"""Tests for the `lazy_load_tables` database setting.

Every test runs once per engine in `ENGINES`, so the same bodies cover `MergeTree` and
`ReplicatedMergeTree` here and a private build can add `SharedMergeTree` by extending the list.
"""

import json

import pytest

import helpers.kafka.common as k
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "instance",
    main_configs=["configs/lazy.xml"],
    macros={"shard": 1, "replica": "r1"},
    with_kafka=True,
    with_zookeeper=True,
    stay_alive=True,
)

DB = "lazy"

# Database and table engine pairs. A private build adds ("Shared", "SharedMergeTree") here and every
# test below runs against it too.
ENGINES = [("Atomic", "MergeTree"), ("Atomic", "ReplicatedMergeTree")]

# Re-reading the metadata is what turns the tables back into unloaded proxies.
RELOAD = f"DETACH DATABASE {DB}; ATTACH DATABASE {DB};"


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(params=ENGINES, ids=[table for _, table in ENGINES])
def engine(started_cluster, request):
    """The table engine to use, with a matching database already created."""
    db_engine, table_engine = request.param
    node.query(f"DROP DATABASE IF EXISTS {DB} SYNC")
    node.query(f"CREATE DATABASE {DB} ENGINE = {db_engine} SETTINGS lazy_load_tables = 1")
    yield table_engine
    # SYNC so the replicated tables release their coordination paths before the next test reuses them.
    node.query(f"DROP DATABASE IF EXISTS {DB} SYNC")


def loaded(table):
    """`system.tables.is_loaded`, which is the only thing that marks a table as deferred."""
    return node.query(
        f"SELECT is_loaded FROM system.tables WHERE database = '{DB}' AND name = '{table}'"
    ).strip()


def test_deferred_table_reports_its_engine(engine):
    """The proxy is an implementation detail, so a deferred table looks like any other except for
    `is_loaded`, which is how a client tells "no sorting key" from "not known yet"."""
    node.query(
        f"""
        CREATE TABLE {DB}.t (id UInt64) ENGINE = {engine} ORDER BY id;
        CREATE TABLE {DB}.eager (id UInt64) ENGINE = Memory;
        {RELOAD}
        """
    )

    engine_of_t = node.query(
        f"SELECT engine FROM system.tables WHERE database = '{DB}' AND name = 't'"
    ).strip()
    assert engine_of_t == engine
    assert loaded("t") == "0"
    assert loaded("eager") == "1"
    # Reading the column must not itself load the table.
    assert loaded("t") == "0"

    assert node.query(f"SELECT count() FROM {DB}.t").strip() == "0"
    assert loaded("t") == "1"

    # A temporary table is never proxied. Both statements share one session so it still exists.
    temporary = node.query(
        "CREATE TEMPORARY TABLE tmp (id UInt64) ENGINE = Memory;"
        "SELECT is_loaded FROM system.tables WHERE is_temporary AND name = 'tmp';"
    ).strip()
    assert temporary == "1"


def test_only_mergetree_family_opts_in(started_cluster):
    """Deferring an engine that works in the background, like a message queue, would cancel that
    work rather than delay it, so opting in is per engine and only the MergeTree family has."""
    opted_in = node.query(
        "SELECT name FROM system.table_engines WHERE supports_deferred_load ORDER BY name"
    ).split()
    assert opted_in, "the MergeTree family must be deferrable"
    assert all(name.endswith("MergeTree") for name in opted_in), opted_in


def test_other_engines_stay_eager(engine):
    """Engines reached by casting to a concrete storage type must not be deferred."""
    node.query(
        f"""
        CREATE TABLE {DB}.src (id UInt64) ENGINE = {engine} ORDER BY id;
        CREATE TABLE {DB}.st (id UInt64) ENGINE = Set;
        CREATE TABLE {DB}.jn (k UInt64, v String) ENGINE = Join(ANY, LEFT, k);
        CREATE TABLE {DB}.mem (id UInt64) ENGINE = Memory;
        CREATE TABLE {DB}.lg (id UInt64) ENGINE = Log;
        CREATE TABLE {DB}.tlg (id UInt64) ENGINE = TinyLog;
        CREATE TABLE {DB}.slg (id UInt64) ENGINE = StripeLog;
        CREATE TABLE {DB}.nul (id UInt64) ENGINE = Null;
        CREATE TABLE {DB}.km (k String, v String) ENGINE = KeeperMap('/lazy_km') PRIMARY KEY k;
        CREATE TABLE {DB}.mrg (id UInt64) ENGINE = Merge('{DB}', 'src');
        CREATE TABLE {DB}.buf (id UInt64) ENGINE = Buffer('{DB}', 'src', 1, 100, 100, 10, 100, 10000, 1000000);
        CREATE TABLE {DB}.dist (id UInt64) ENGINE = Distributed('single_shard_cluster', '{DB}', 'src');
        {RELOAD}
        """
    )

    deferred = node.query(
        f"SELECT name FROM system.tables WHERE database = '{DB}' AND NOT is_loaded"
    ).split()
    assert deferred == ["src"], deferred


def test_kafka_ingests_after_restart(engine):
    """A Kafka table feeding a view must ingest after a restart with nobody reading it."""
    admin_client = k.get_admin_client(cluster)
    topic = "lazy_kafka_topic"
    k.kafka_create_topic(admin_client, topic)
    try:
        node.query(
            f"""
            CREATE TABLE {DB}.kafka_src (key UInt64, value UInt64) ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092', kafka_topic_list = '{topic}',
                     kafka_group_name = '{topic}_group', kafka_format = 'JSONEachRow';
            CREATE TABLE {DB}.dest (key UInt64, value UInt64) ENGINE = {engine} ORDER BY key;
            CREATE MATERIALIZED VIEW {DB}.mv TO {DB}.dest AS SELECT key, value FROM {DB}.kafka_src;
            {RELOAD}
            """
        )
        assert loaded("kafka_src") == "1"

        k.kafka_produce(
            cluster, topic, [json.dumps({"key": i, "value": i * 2}) for i in range(50)]
        )

        # Nothing reads kafka_src: only the engine's own stream can move these rows.
        node.query_with_retry(
            f"SELECT count() FROM {DB}.dest",
            check_callback=lambda result: int(result) == 50,
            retry_count=60,
            sleep_time=1,
        )
        assert int(node.query(f"SELECT sum(value) FROM {DB}.dest")) == sum(
            i * 2 for i in range(50)
        )
    finally:
        k.kafka_delete_topic(admin_client, topic)


def test_mutations(engine):
    """Mutations are validated against the metadata and dispatched through a MergeTreeData cast,
    both of which have to reach the real storage."""
    node.query(
        f"""
        CREATE TABLE {DB}.t (id UInt64, v String, n UInt32,
            INDEX idx_v v TYPE bloom_filter GRANULARITY 1,
            PROJECTION proj (SELECT n, count() GROUP BY n))
        ENGINE = {engine} ORDER BY id SETTINGS lightweight_mutation_projection_mode = 'rebuild';
        INSERT INTO {DB}.t SELECT number, toString(number), number % 5 FROM numbers(10);
        {RELOAD}
        ALTER TABLE {DB}.t DELETE WHERE id = 0 SETTINGS mutations_sync = 2;
        {RELOAD}
        DELETE FROM {DB}.t WHERE id = 1;
        {RELOAD}
        ALTER TABLE {DB}.t MATERIALIZE INDEX idx_v SETTINGS mutations_sync = 2;
        {RELOAD}
        ALTER TABLE {DB}.t MATERIALIZE PROJECTION proj SETTINGS mutations_sync = 2;
        {RELOAD}
        ALTER TABLE {DB}.t UPDATE v = 'x' WHERE id = 2 SETTINGS mutations_sync = 2;
        """
    )
    assert int(node.query(f"SELECT count() FROM {DB}.t")) == 8
    assert node.query(f"SELECT v FROM {DB}.t WHERE id = 2").strip() == "x"

    # Rejecting an update of a key column needs the sorting key, which the proxy does not have.
    node.query(RELOAD)
    assert loaded("t") == "0"
    assert "CANNOT_UPDATE_COLUMN" in node.query_and_get_error(
        f"ALTER TABLE {DB}.t UPDATE id = id + 1 WHERE 1"
    )


def test_partition_scoped_mutation(engine):
    """`ALTER ... IN PARTITION` resolves the partition through a MergeTreeData cast."""
    node.query(
        f"""
        CREATE TABLE {DB}.t (d Date, id UInt64) ENGINE = {engine}
        PARTITION BY toYYYYMM(d) ORDER BY id;
        INSERT INTO {DB}.t VALUES ('2026-01-01', 1), ('2026-02-01', 2);
        {RELOAD}
        """
    )
    assert loaded("t") == "0"
    node.query(
        f"ALTER TABLE {DB}.t DELETE IN PARTITION 202601 WHERE id = 1 SETTINGS mutations_sync = 2"
    )
    assert int(node.query(f"SELECT count() FROM {DB}.t")) == 1


def test_lightweight_delete_projection_guard(engine):
    """With `lightweight_mutation_projection_mode = throw` the DELETE must be rejected. Missing the
    check does not just skip a validation: MutateTask then treats THROW like DROP."""
    node.query(
        f"""
        CREATE TABLE {DB}.t (id UInt64, n UInt32, PROJECTION proj (SELECT n, count() GROUP BY n))
        ENGINE = {engine} ORDER BY id SETTINGS lightweight_mutation_projection_mode = 'throw';
        INSERT INTO {DB}.t SELECT number, number % 5 FROM numbers(10);
        {RELOAD}
        """
    )
    assert loaded("t") == "0"
    assert "SUPPORT_IS_DISABLED" in node.query_and_get_error(
        f"DELETE FROM {DB}.t WHERE id = 1"
    )

    # The projection must still be there, and the rows untouched.
    surviving = node.query(
        f"""SELECT
                (SELECT name FROM system.projections WHERE database = '{DB}' AND table = 't'),
                (SELECT count() FROM {DB}.t)"""
    ).strip()
    assert surviving == "proj\t10"


def test_lightweight_update(engine):
    """`supportsLightweightUpdate` is consulted on the catalog pointer before `updateLightweight`."""
    node.query(
        f"""
        CREATE TABLE {DB}.t (id UInt64, v String) ENGINE = {engine} ORDER BY id
        SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
        INSERT INTO {DB}.t SELECT number, toString(number) FROM numbers(10);
        {RELOAD}
        """
    )
    assert loaded("t") == "0"
    node.query(f"UPDATE {DB}.t SET v = 'updated' WHERE id = 3")
    assert node.query(f"SELECT v FROM {DB}.t WHERE id = 3").strip() == "updated"


def test_alter_modify_ttl(engine):
    """`supportsTTL` gates ALTER MODIFY TTL and defaults to false on the proxy."""
    node.query(
        f"""
        CREATE TABLE {DB}.t (d Date, id UInt64) ENGINE = {engine} ORDER BY id;
        {RELOAD}
        ALTER TABLE {DB}.t MODIFY TTL d + INTERVAL 10 YEAR;
        """
    )
    assert "TTL" in node.query(f"SHOW CREATE TABLE {DB}.t")


def test_create_as_copies_full_structure(engine):
    """`CREATE TABLE ... AS` copies indices, projections, constraints and the comment out of the
    source's metadata, which a deferred table reports as columns only."""
    node.query(
        f"""
        CREATE TABLE {DB}.src (id UInt64, n UInt32, CONSTRAINT c_n CHECK n > 0,
            INDEX idx_n n TYPE minmax GRANULARITY 1,
            PROJECTION p (SELECT n, count() GROUP BY n))
        ENGINE = {engine} ORDER BY id COMMENT 'source comment';
        {RELOAD}
        """
    )
    assert loaded("src") == "0"

    # An explicit engine, since the source's stored one carries its own coordination path.
    node.query(f"CREATE TABLE {DB}.copy AS {DB}.src ENGINE = {engine} ORDER BY id")
    copied = node.query(
        f"""SELECT
                (SELECT count() FROM system.data_skipping_indices WHERE database = '{DB}' AND table = 'copy'),
                (SELECT count() FROM system.projections WHERE database = '{DB}' AND table = 'copy'),
                (SELECT comment FROM system.tables WHERE database = '{DB}' AND name = 'copy')"""
    ).strip()
    assert copied == "1\t1\tsource comment"
    assert "VIOLATED_CONSTRAINT" in node.query_and_get_error(
        f"INSERT INTO {DB}.copy VALUES (1, 0)"
    )

    # CLONE AS accepts only the MergeTree family, which it decides from the engine name.
    node.query(RELOAD)
    assert loaded("src") == "0"
    node.query(f"CREATE TABLE {DB}.clone CLONE AS {DB}.src ENGINE = {engine} ORDER BY id")


def test_system_tables_report_nothing_until_loaded(engine):
    """The system tables that walk every table reach the real storage without loading it, so they
    report nothing until something else loads it."""
    node.query(
        f"""
        CREATE TABLE {DB}.t (id UInt64, v String, INDEX idx_v v TYPE bloom_filter GRANULARITY 1)
        ENGINE = {engine} ORDER BY id;
        INSERT INTO {DB}.t SELECT number, toString(number) FROM numbers(1000);
        {RELOAD}
        """
    )

    before = node.query(
        f"""SELECT
                (SELECT count() FROM system.parts WHERE database = '{DB}'),
                (SELECT count() FROM system.data_skipping_indices WHERE database = '{DB}')"""
    ).strip()
    assert before == "0\t0"
    assert loaded("t") == "0", "listing the system tables must not load the table"

    assert int(node.query(f"SELECT count() FROM {DB}.t")) == 1000
    after = node.query(
        f"""SELECT
                (SELECT count() FROM system.parts WHERE database = '{DB}' AND active),
                (SELECT sum(rows) FROM system.parts WHERE database = '{DB}' AND active),
                (SELECT total_rows FROM system.tables WHERE database = '{DB}' AND name = 't'),
                (SELECT name FROM system.data_skipping_indices WHERE database = '{DB}'),
                (SELECT data_compressed_bytes > 0 AND marks_bytes > 0
                 FROM system.data_skipping_indices WHERE database = '{DB}')"""
    ).strip()
    assert after == "1\t1000\t1000\tidx_v\t1"

    node.query(f"ALTER TABLE {DB}.t DELETE WHERE id = 0 SETTINGS mutations_sync = 2")
    assert int(node.query(f"SELECT count() FROM system.mutations WHERE database = '{DB}'")) >= 1


def test_system_commands_on_deferred_table(engine):
    """The SYSTEM commands that name a table reach it through a MergeTreeData cast."""
    node.query(
        f"""
        CREATE TABLE {DB}.t (id UInt64) ENGINE = {engine} ORDER BY id
        SETTINGS merge_selector_algorithm = 'Manual';
        INSERT INTO {DB}.t SELECT number FROM numbers(10);
        INSERT INTO {DB}.t SELECT number + 10 FROM numbers(10);
        """
    )
    parts = node.query(
        f"SELECT name FROM system.parts WHERE database = '{DB}' AND active ORDER BY name"
    ).split()
    assert len(parts) == 2, parts

    part_list = ", ".join(repr(part) for part in parts)

    node.query(RELOAD)
    assert loaded("t") == "0"
    node.query(f"OPTIMIZE TABLE {DB}.t DRY RUN PARTS {part_list}")

    node.query(RELOAD)
    node.query(f"SYSTEM UNLOAD PRIMARY KEY {DB}.t; SYSTEM LOAD PRIMARY KEY {DB}.t;")

    node.query(RELOAD)
    node.query(f"SYSTEM WAIT LOADING PARTS {DB}.t; SYSTEM PREWARM MARK CACHE {DB}.t;")

    # Merging the parts last, since it invalidates the names collected above.
    node.query(RELOAD)
    node.query(f"SYSTEM SCHEDULE MERGE {DB}.t PARTS {part_list}")


def test_merge_tree_table_functions(engine):
    """The table functions that read parts directly cast the source table to MergeTreeData."""
    node.query(
        f"""
        CREATE TABLE {DB}.t (id UInt64, n UInt32, PROJECTION p (SELECT n, count() GROUP BY n))
        ENGINE = {engine} ORDER BY id;
        INSERT INTO {DB}.t VALUES (1, 7);
        {RELOAD}
        """
    )
    assert loaded("t") == "0"
    assert int(node.query(f"SELECT count() FROM mergeTreeIndex('{DB}', 't')")) > 0
    assert int(node.query(f"SELECT count() FROM mergeTreeProjection('{DB}', 't', 'p')")) > 0
    assert int(node.query(f"SELECT count() FROM mergeTreeCodecBlockCounts('{DB}', 't')")) > 0


def test_hypothetical_index(engine):
    """`CREATE HYPOTHETICAL INDEX` is rejected unless the table casts to MergeTreeData."""
    node.query(
        f"""
        CREATE TABLE {DB}.t (id UInt64, n UInt32) ENGINE = {engine} ORDER BY id;
        INSERT INTO {DB}.t VALUES (1, 7);
        {RELOAD}
        """
    )
    assert loaded("t") == "0"
    node.query(f"CREATE HYPOTHETICAL INDEX h ON {DB}.t (n) TYPE minmax")


def test_nested_column_alters(engine):
    """`share_nested_offsets` comes from the engine settings, and defaulting it to true rejects
    renames that the setting allows."""
    node.query(
        f"""
        CREATE TABLE {DB}.t (id UInt64, nest Nested(a UInt8, b String)) ENGINE = {engine}
        ORDER BY id SETTINGS share_nested_offsets = 0;
        INSERT INTO {DB}.t VALUES (1, [1], ['x']);
        {RELOAD}
        """
    )
    assert loaded("t") == "0"

    # With the setting off there is no Nested group, so the whole struct is simply not a column.
    assert "NOT_FOUND_COLUMN_IN_BLOCK" in node.query_and_get_error(
        f"ALTER TABLE {DB}.t RENAME COLUMN nest TO other"
    )
    node.query(RELOAD)
    assert "NOT_FOUND_COLUMN_IN_BLOCK" in node.query_and_get_error(
        f"ALTER TABLE {DB}.t DROP COLUMN nest"
    )


def test_system_graphite_retentions(engine):
    """`system.graphite_retentions` reaches GraphiteMergeTree through a MergeTreeData cast."""
    if engine != "MergeTree":
        pytest.skip("the Graphite section is specific to plain GraphiteMergeTree")

    node.query(
        f"""
        CREATE TABLE {DB}.graphite (metric String, value Float64, timestamp UInt32, date Date, updated UInt32)
        ENGINE = GraphiteMergeTree('graphite_rollup') ORDER BY (metric, timestamp);
        {RELOAD}
        """
    )
    assert loaded("graphite") == "0"
    assert int(node.query(f"SELECT count() FROM {DB}.graphite")) == 0
    retentions = node.query(
        f"SELECT count() FROM system.graphite_retentions "
        f"WHERE has(Tables.database, '{DB}') AND has(Tables.table, 'graphite')"
    )
    assert int(retentions) > 0


def test_async_insert_table_setting(engine):
    """The table-level `async_insert` setting is read through `areAsynchronousInsertsEnabled`."""
    node.query(
        f"""
        CREATE TABLE {DB}.t (id UInt64) ENGINE = {engine} ORDER BY id SETTINGS async_insert = 1;
        {RELOAD}
        """
    )
    assert loaded("t") == "0"

    query_id = f"lazy_async_insert_{engine}"
    node.query(f"INSERT INTO {DB}.t SETTINGS async_insert = 0 VALUES (1)", query_id=query_id)
    node.query("SYSTEM FLUSH LOGS asynchronous_insert_log")
    logged = node.query(
        f"SELECT count() FROM system.asynchronous_insert_log WHERE query_id = '{query_id}'"
    )
    assert int(logged) == 1


def test_trivial_count_and_parallel_replicas(engine):
    """The planner asks the storage whether it is a MergeTree and whether trivial count applies."""
    node.query(
        f"""
        CREATE TABLE {DB}.t (id UInt64, v UInt64) ENGINE = {engine} ORDER BY id;
        INSERT INTO {DB}.t SELECT number, number * 2 FROM numbers(1000);
        {RELOAD}
        """
    )
    plan = node.query(f"EXPLAIN SELECT count() FROM {DB}.t")
    assert "Optimized trivial count" in plan, plan

    node.query(RELOAD)
    plan = node.query(
        f"""EXPLAIN SELECT sum(v) FROM {DB}.t SETTINGS
            enable_parallel_replicas = 1, max_parallel_replicas = 3,
            cluster_for_parallel_replicas = 'parallel_replicas_cluster',
            parallel_replicas_for_non_replicated_merge_tree = 1,
            parallel_replicas_min_number_of_rows_per_replica = 0"""
    )
    assert "ParallelReplicas" in plan, plan


def test_partition_moves_between_tables(engine):
    """Both sides of MOVE PARTITION and ATTACH PARTITION FROM are cast to MergeTreeData."""
    node.query(
        f"""
        CREATE TABLE {DB}.src (d Date, id UInt64) ENGINE = {engine} PARTITION BY toYYYYMM(d) ORDER BY id;
        CREATE TABLE {DB}.dst (d Date, id UInt64) ENGINE = {engine} PARTITION BY toYYYYMM(d) ORDER BY id;
        INSERT INTO {DB}.src VALUES ('2024-01-01', 1), ('2024-02-01', 2);
        {RELOAD}
        """
    )
    assert loaded("src") == "0" and loaded("dst") == "0"

    node.query(f"ALTER TABLE {DB}.src MOVE PARTITION 202401 TO TABLE {DB}.dst")
    node.query(f"ALTER TABLE {DB}.dst ATTACH PARTITION 202402 FROM {DB}.src")
    moved = node.query(
        f"SELECT (SELECT count() FROM {DB}.dst), (SELECT count() FROM {DB}.src)"
    ).strip()
    assert moved == "2\t1"


def test_backup_restore_and_incremental(engine):
    """`IStorage::backupData` is a no-op, so an unforwarded proxy backs up nothing and the
    incremental backup then diffs against an empty base."""
    node.query(
        f"""
        CREATE TABLE {DB}.t (id UInt64) ENGINE = {engine} ORDER BY id;
        INSERT INTO {DB}.t SELECT number FROM numbers(100);
        {RELOAD}
        """
    )
    assert loaded("t") == "0"

    base, incremental = f"lazy_base_{engine}", f"lazy_incr_{engine}"
    node.query(f"BACKUP TABLE {DB}.t TO Disk('backups', '{base}')")
    node.query(
        f"""
        INSERT INTO {DB}.t SELECT number + 100 FROM numbers(50);
        {RELOAD}
        """
    )
    node.query(
        f"BACKUP TABLE {DB}.t TO Disk('backups', '{incremental}') "
        f"SETTINGS base_backup = Disk('backups', '{base}')"
    )

    # Restoring under a new name would reuse the replicated table's coordination path.
    node.query(f"DROP TABLE {DB}.t SYNC")
    node.query(f"RESTORE TABLE {DB}.t FROM Disk('backups', '{incremental}')")
    assert int(node.query(f"SELECT count() FROM {DB}.t")) == 150


def test_backup_partitions(engine):
    """Partition-level backup also needs `supportsBackupPartition` from the real storage."""
    node.query(
        f"""
        CREATE TABLE {DB}.t (d Date, id UInt64) ENGINE = {engine} PARTITION BY toYYYYMM(d) ORDER BY id;
        INSERT INTO {DB}.t SELECT '2024-01-01', number FROM numbers(500);
        INSERT INTO {DB}.t SELECT '2024-02-01', number FROM numbers(300);
        {RELOAD}
        """
    )
    assert loaded("t") == "0"

    backup = f"lazy_part_backup_{engine}"
    node.query(f"BACKUP TABLE {DB}.t PARTITIONS '202401' TO Disk('backups', '{backup}')")
    node.query(f"DROP TABLE {DB}.t SYNC")
    node.query(f"RESTORE TABLE {DB}.t FROM Disk('backups', '{backup}')")
    assert int(node.query(f"SELECT count() FROM {DB}.t")) == 500


def test_backup_database(engine):
    """A whole-database backup collects each table through the same `backupData` call."""
    node.query(
        f"""
        CREATE TABLE {DB}.a (id UInt64) ENGINE = {engine} ORDER BY id;
        CREATE TABLE {DB}.b (id UInt64) ENGINE = {engine} ORDER BY id;
        INSERT INTO {DB}.a SELECT number FROM numbers(500);
        INSERT INTO {DB}.b SELECT number FROM numbers(700);
        {RELOAD}
        """
    )
    assert loaded("a") == "0" and loaded("b") == "0"

    backup = f"lazy_db_backup_{engine}"
    node.query(f"BACKUP DATABASE {DB} TO Disk('backups', '{backup}')")
    node.query(f"DROP DATABASE {DB} SYNC")
    node.query(f"RESTORE DATABASE {DB} FROM Disk('backups', '{backup}')")

    restored = node.query(
        f"SELECT (SELECT count() FROM {DB}.a), (SELECT count() FROM {DB}.b)"
    ).strip()
    assert restored == "500\t700"


def test_system_replicas_after_access(engine):
    """`system.replicas` casts to StorageReplicatedMergeTree."""
    if engine == "MergeTree":
        pytest.skip("not a replicated engine")

    node.query(
        f"""
        CREATE TABLE {DB}.t (id UInt64) ENGINE = {engine} ORDER BY id;
        INSERT INTO {DB}.t SELECT number FROM numbers(10);
        {RELOAD}
        """
    )
    assert int(node.query(f"SELECT count() FROM {DB}.t")) == 10
    assert int(node.query(f"SELECT count() FROM system.replicas WHERE database = '{DB}'")) == 1


def test_replica_commands_on_deferred_table(engine):
    """SYSTEM SYNC REPLICA and the /replicas_status handler both cast to the replicated storage."""
    if engine == "MergeTree":
        pytest.skip("not a replicated engine")

    node.query(
        f"""
        CREATE TABLE {DB}.t (id UInt64) ENGINE = {engine} ORDER BY id;
        {RELOAD}
        """
    )
    assert loaded("t") == "0"
    node.query(f"SYSTEM SYNC REPLICA {DB}.t")
    assert "Ok" in node.http_request("replicas_status", method="GET").text

    node.query(RELOAD)
    node.query(f"SYSTEM RESTART REPLICA {DB}.t")
    assert int(node.query(f"SELECT count() FROM system.replication_queue WHERE database = '{DB}'")) == 0

    # The replica-divergence check is skipped for anything that is not named `Replicated*`.
    node.query(RELOAD)
    assert loaded("t") == "0"
    assert "BAD_ARGUMENTS" in node.query_and_get_error(
        f"ALTER TABLE {DB}.t UPDATE id = rand() WHERE 1"
    )
