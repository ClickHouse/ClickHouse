"""Tests for the `lazy_load_tables` database setting that a stateless test cannot cover: a Kafka table
ingesting with nobody reading it, which needs the Kafka service, and a walk over every system table,
which other tests on a shared server could disturb by loading the table. The rest is covered by the
`*_lazy_load_tables_*` stateless tests.

A test that takes the `engine` fixture runs once per engine in `ENGINES`, so a private build can add
`SharedMergeTree` by extending the list.
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


def create_lazy_database(db_engine="Atomic"):
    node.query(f"DROP DATABASE IF EXISTS {DB} SYNC")
    node.query(f"CREATE DATABASE {DB} ENGINE = {db_engine} SETTINGS lazy_load_tables = 1")


@pytest.fixture
def lazy_database(started_cluster):
    """A database that defers loading, for a test that does not vary the table engine."""
    create_lazy_database()
    yield
    # SYNC so the replicated tables release their coordination paths before the next test reuses them.
    node.query(f"DROP DATABASE IF EXISTS {DB} SYNC")


@pytest.fixture(params=ENGINES, ids=[table for _, table in ENGINES])
def engine(started_cluster, request):
    """The table engine to use, in a database created with the matching engine."""
    db_engine, table_engine = request.param
    create_lazy_database(db_engine)
    yield table_engine
    node.query(f"DROP DATABASE IF EXISTS {DB} SYNC")


def loaded(table):
    """`system.tables.is_loaded`, which is the only thing that marks a table as deferred."""
    return node.query(
        f"SELECT is_loaded FROM system.tables WHERE database = '{DB}' AND name = '{table}'"
    ).strip()


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
            CREATE TABLE {DB}.kafka_unused (key UInt64, value UInt64) ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092', kafka_topic_list = '{topic}_unused',
                     kafka_group_name = '{topic}_unused_group', kafka_format = 'JSONEachRow';
            CREATE TABLE {DB}.dest (key UInt64, value UInt64) ENGINE = {engine} ORDER BY key;
            CREATE MATERIALIZED VIEW {DB}.mv TO {DB}.dest AS SELECT key, value FROM {DB}.kafka_src;
            {RELOAD}
            """
        )
        assert loaded("kafka_src") == "1"
        # Nothing depends on this one, so its consumer would feed nothing and it stays deferred.
        assert loaded("kafka_unused") == "0"

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


# Endless generators, which a `SELECT *` would never come back from.
ENDLESS_SYSTEM_TABLES = {"numbers", "numbers_mt", "zeros", "zeros_mt", "generate_series", "primes"}


def test_no_system_table_loads_a_lazy_table(lazy_database):
    """Reading a system table is an observation, so none of them may materialize a table. This walks
    every system table rather than the few known to reach into storages, so a column added later that
    resolves the proxy is caught here instead of in production."""
    node.query(
        f"""
        CREATE TABLE {DB}.t (d Date, id UInt64, s String,
            INDEX idx_s s TYPE bloom_filter GRANULARITY 1,
            PROJECTION p (SELECT s, count() GROUP BY s))
        ENGINE = MergeTree PARTITION BY toYYYYMM(d) ORDER BY id;
        INSERT INTO {DB}.t VALUES ('2026-01-01', 1, 'a');
        {RELOAD}
        """
    )
    assert loaded("t") == "0"

    names = [
        name
        for name in node.query(
            "SELECT name FROM system.tables WHERE database = 'system' ORDER BY name"
        ).split()
        if name not in ENDLESS_SYSTEM_TABLES
    ]
    assert len(names) > 50, names

    def read_all(table):
        # Some refuse to be read without a predicate, and a new endless one must not hang the
        # test, neither of which is what this checks.
        node.query(
            f"SELECT * FROM system.{table} FORMAT Null SETTINGS max_execution_time = 30",
            ignore_error=True,
        )

    for name in names:
        read_all(name)
    if loaded("t") == "0":
        return

    # Something materialized it, so go again and name every table that does.
    loaders = []
    for name in names:
        node.query(RELOAD)
        read_all(name)
        if loaded("t") != "0":
            loaders.append(name)
    assert not loaders, f"these system tables loaded the table: {loaders}"
