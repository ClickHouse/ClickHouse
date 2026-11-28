"""
Tests for kafka_partition_assignment='shard_sticky' mode.

Verifies that when a Kafka table is configured with explicit partition ownership,
it consumes only from its assigned partitions (no cross-shard duplication) and
that two tables with disjoint partition sets together cover all data exactly once.
"""

import json
import logging
import time

import pytest
from kafka import KafkaProducer

from helpers.kafka.common_direct import *
import helpers.kafka.common as k

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "instance",
    main_configs=["configs/kafka.xml"],
    user_configs=["configs/users.xml"],
    with_kafka=True,
    with_zookeeper=False,
    macros={
        "kafka_broker": "kafka1",
        "kafka_topic_old": k.KAFKA_TOPIC_OLD,
        "kafka_group_name_old": k.KAFKA_CONSUMER_GROUP_OLD,
        "kafka_topic_new": k.KAFKA_TOPIC_NEW,
        "kafka_group_name_new": k.KAFKA_CONSUMER_GROUP_NEW,
        "kafka_client_id": "instance",
        "kafka_format_json_each_row": "JSONEachRow",
    },
    clickhouse_path_dir="clickhouse_path",
)


@pytest.fixture(scope="module")
def kafka_cluster():
    try:
        cluster.start()
        kafka_id = instance.cluster.kafka_docker_id
        logging.debug(f"kafka_id is {kafka_id}")
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def kafka_setup_teardown():
    instance.query("DROP DATABASE IF EXISTS test SYNC; CREATE DATABASE test;")
    admin_client = k.get_admin_client(cluster)
    topics = [t for t in admin_client.list_topics() if not t.startswith("_")]
    if topics:
        result = admin_client.delete_topics(topics)
        for topic, error in result.topic_error_codes:
            if error != 0:
                logging.warning(f"Error {error} deleting topic {topic}")
    yield


def _produce_to_partition(kafka_cluster, topic, partition, messages):
    """Produce a list of message dicts to a specific Kafka partition."""
    producer = KafkaProducer(
        bootstrap_servers=f"localhost:{kafka_cluster.kafka_port}",
        value_serializer=lambda v: json.dumps(v).encode(),
    )
    for msg in messages:
        producer.send(topic, value=msg, partition=partition).get(timeout=10)
    producer.flush()
    producer.close()


# ---------------------------------------------------------------------------
# Test 1: Basic sticky assignment — table reads ONLY its assigned partitions
# ---------------------------------------------------------------------------

def test_kafka_sticky_partition_basic(kafka_cluster):
    """
    A table with kafka_shard_partitions='0,1' on a 4-partition topic must
    consume exactly partitions 0 and 1, never 2 or 3.
    """
    suffix = k.random_string(6)
    topic = f"sticky_basic_{suffix}"
    group = f"sticky_basic_group_{suffix}"

    with k.kafka_topic(k.get_admin_client(kafka_cluster), topic, num_partitions=4):
        try:
            instance.query(
                f"""
                CREATE TABLE test.kafka_shard1 (key UInt64, src_partition UInt8)
                ENGINE = Kafka
                SETTINGS
                    kafka_broker_list        = 'kafka1:19092',
                    kafka_topic_list         = '{topic}',
                    kafka_group_name         = '{group}',
                    kafka_format             = 'JSONEachRow',
                    kafka_num_consumers      = 2,
                    kafka_partition_assignment = 'shard_sticky',
                    kafka_shard_partitions     = '0,1',
                    kafka_replica_consume_mode = 'cooperative_split';

                CREATE TABLE test.events_shard1
                    (key UInt64, src_partition UInt8)
                ENGINE = MergeTree ORDER BY key;

                CREATE MATERIALIZED VIEW test.mv_shard1
                TO test.events_shard1 AS
                SELECT key, src_partition FROM test.kafka_shard1;
                """
            )

            # Produce 3 messages to each of the 4 partitions
            for part in range(4):
                _produce_to_partition(
                    kafka_cluster, topic, part,
                    [{"key": part * 10 + i, "src_partition": part} for i in range(3)],
                )

            # Wait for shard1 to consume its partitions (0,1 → 6 messages)
            result = instance.query_with_retry(
                "SELECT count() FROM test.events_shard1",
                timeout=30,
                sleep_time=1,
                check_callback=lambda r: int(r) == 6,
            )
            assert int(result) == 6, f"Expected 6 rows (partitions 0+1), got {result}"

            # Verify only partitions 0 and 1 are present — never 2 or 3
            seen = instance.query(
                "SELECT DISTINCT src_partition FROM test.events_shard1 ORDER BY src_partition"
            ).strip()
            assert seen == "0\n1", (
                f"Shard1 should only see partitions 0,1 — got: {seen!r}"
            )

        finally:
            instance.query(
                "DROP TABLE IF EXISTS test.mv_shard1 SYNC;"
                "DROP TABLE IF EXISTS test.kafka_shard1 SYNC;"
                "DROP TABLE IF EXISTS test.events_shard1 SYNC;"
            )


# ---------------------------------------------------------------------------
# Test 2: Zero cross-shard duplication — two tables cover all partitions exactly once
# ---------------------------------------------------------------------------

def test_kafka_sticky_partition_no_cross_shard_duplication(kafka_cluster):
    """
    Two tables with disjoint partition ownership (shard1: 0,1 / shard2: 2,3)
    must together cover all 4 partitions with zero duplication.
    Every event_id appears in exactly one table.
    """
    suffix = k.random_string(6)
    topic = f"sticky_no_dup_{suffix}"
    group = f"sticky_no_dup_group_{suffix}"

    with k.kafka_topic(k.get_admin_client(kafka_cluster), topic, num_partitions=4):
        try:
            instance.query(
                f"""
                -- Shard 1: owns partitions 0, 1
                CREATE TABLE test.kafka_shard1 (key UInt64, src_partition UInt8)
                ENGINE = Kafka
                SETTINGS
                    kafka_broker_list          = 'kafka1:19092',
                    kafka_topic_list           = '{topic}',
                    kafka_group_name           = '{group}_s1',
                    kafka_format               = 'JSONEachRow',
                    kafka_partition_assignment = 'shard_sticky',
                    kafka_shard_partitions     = '0,1';

                CREATE TABLE test.events_shard1
                    (key UInt64, src_partition UInt8)
                ENGINE = MergeTree ORDER BY key;

                CREATE MATERIALIZED VIEW test.mv_shard1
                TO test.events_shard1 AS
                SELECT key, src_partition FROM test.kafka_shard1;

                -- Shard 2: owns partitions 2, 3
                CREATE TABLE test.kafka_shard2 (key UInt64, src_partition UInt8)
                ENGINE = Kafka
                SETTINGS
                    kafka_broker_list          = 'kafka1:19092',
                    kafka_topic_list           = '{topic}',
                    kafka_group_name           = '{group}_s2',
                    kafka_format               = 'JSONEachRow',
                    kafka_partition_assignment = 'shard_sticky',
                    kafka_shard_partitions     = '2,3';

                CREATE TABLE test.events_shard2
                    (key UInt64, src_partition UInt8)
                ENGINE = MergeTree ORDER BY key;

                CREATE MATERIALIZED VIEW test.mv_shard2
                TO test.events_shard2 AS
                SELECT key, src_partition FROM test.kafka_shard2;
                """
            )

            total_msgs = 0
            for part in range(4):
                msgs = [{"key": part * 100 + i, "src_partition": part} for i in range(5)]
                _produce_to_partition(kafka_cluster, topic, part, msgs)
                total_msgs += len(msgs)
            # total_msgs = 20

            # Wait until both shards have consumed all their messages (5 each)
            instance.query_with_retry(
                "SELECT count() FROM test.events_shard1",
                timeout=30, sleep_time=1,
                check_callback=lambda r: int(r) == 10,
            )
            instance.query_with_retry(
                "SELECT count() FROM test.events_shard2",
                timeout=30, sleep_time=1,
                check_callback=lambda r: int(r) == 10,
            )

            # Partition isolation
            s1_parts = instance.query(
                "SELECT DISTINCT src_partition FROM test.events_shard1 ORDER BY src_partition"
            ).strip()
            s2_parts = instance.query(
                "SELECT DISTINCT src_partition FROM test.events_shard2 ORDER BY src_partition"
            ).strip()
            assert s1_parts == "0\n1", f"Shard1 partition mismatch: {s1_parts!r}"
            assert s2_parts == "2\n3", f"Shard2 partition mismatch: {s2_parts!r}"

            # Zero duplication: no key appears in both tables
            dup_count = instance.query(
                """
                SELECT count() FROM (
                    SELECT key FROM test.events_shard1
                    INTERSECT
                    SELECT key FROM test.events_shard2
                )
                """
            ).strip()
            assert dup_count == "0", (
                f"Found {dup_count} duplicate keys across shards — expected 0"
            )

            # Full coverage: union of both tables has all total_msgs rows
            total = instance.query(
                """
                SELECT count() FROM (
                    SELECT key FROM test.events_shard1
                    UNION ALL
                    SELECT key FROM test.events_shard2
                )
                """
            ).strip()
            assert int(total) == total_msgs, (
                f"Expected {total_msgs} total rows across shards, got {total}"
            )

        finally:
            for tbl in ("mv_shard1", "kafka_shard1", "events_shard1",
                        "mv_shard2", "kafka_shard2", "events_shard2"):
                instance.query(f"DROP TABLE IF EXISTS test.{tbl} SYNC;")


# ---------------------------------------------------------------------------
# Test 3: cooperative_split — consumers divide the shard's partitions evenly
# ---------------------------------------------------------------------------

def test_kafka_sticky_partition_cooperative_split(kafka_cluster):
    """
    With kafka_num_consumers=2, kafka_shard_partitions='0,1,2,3', and
    kafka_replica_consume_mode='cooperative_split', each consumer reads
    exactly 2 partitions (round-robin split). All 4 partitions are covered
    and no message is lost or duplicated.
    """
    suffix = k.random_string(6)
    topic = f"sticky_coop_{suffix}"
    group = f"sticky_coop_group_{suffix}"

    with k.kafka_topic(k.get_admin_client(kafka_cluster), topic, num_partitions=4):
        try:
            instance.query(
                f"""
                CREATE TABLE test.kafka_coop (key UInt64, src_partition UInt8)
                ENGINE = Kafka
                SETTINGS
                    kafka_broker_list          = 'kafka1:19092',
                    kafka_topic_list           = '{topic}',
                    kafka_group_name           = '{group}',
                    kafka_format               = 'JSONEachRow',
                    kafka_num_consumers        = 2,
                    kafka_partition_assignment = 'shard_sticky',
                    kafka_shard_partitions     = '0,1,2,3',
                    kafka_replica_consume_mode = 'cooperative_split';

                CREATE TABLE test.events_coop
                    (key UInt64, src_partition UInt8)
                ENGINE = MergeTree ORDER BY key;

                CREATE MATERIALIZED VIEW test.mv_coop
                TO test.events_coop AS
                SELECT key, src_partition FROM test.kafka_coop;
                """
            )

            for part in range(4):
                _produce_to_partition(
                    kafka_cluster, topic, part,
                    [{"key": part * 10 + i, "src_partition": part} for i in range(5)],
                )

            result = instance.query_with_retry(
                "SELECT count() FROM test.events_coop",
                timeout=30, sleep_time=1,
                check_callback=lambda r: int(r) == 20,
            )
            assert int(result) == 20, f"Expected 20 rows (all 4 partitions), got {result}"

            # All 4 partitions covered
            seen_parts = instance.query(
                "SELECT DISTINCT src_partition FROM test.events_coop ORDER BY src_partition"
            ).strip()
            assert seen_parts == "0\n1\n2\n3", (
                f"Expected all 4 partitions, got: {seen_parts!r}"
            )

            # No duplicates within the shard
            dup = instance.query(
                "SELECT count() FROM (SELECT key, count() AS c FROM test.events_coop GROUP BY key HAVING c > 1)"
            ).strip()
            assert dup == "0", f"Found {dup} duplicate keys in cooperative_split mode"

        finally:
            instance.query(
                "DROP TABLE IF EXISTS test.mv_coop SYNC;"
                "DROP TABLE IF EXISTS test.kafka_coop SYNC;"
                "DROP TABLE IF EXISTS test.events_coop SYNC;"
            )


# ---------------------------------------------------------------------------
# Test 4: redundant mode — every consumer reads all shard partitions
# ---------------------------------------------------------------------------

def test_kafka_sticky_partition_redundant_mode(kafka_cluster):
    """
    With kafka_replica_consume_mode='redundant', every consumer reads all
    assigned partitions. All messages are still consumed (fault tolerant).
    """
    suffix = k.random_string(6)
    topic = f"sticky_redundant_{suffix}"
    group = f"sticky_redundant_group_{suffix}"

    with k.kafka_topic(k.get_admin_client(kafka_cluster), topic, num_partitions=2):
        try:
            instance.query(
                f"""
                CREATE TABLE test.kafka_redundant (key UInt64, src_partition UInt8)
                ENGINE = Kafka
                SETTINGS
                    kafka_broker_list          = 'kafka1:19092',
                    kafka_topic_list           = '{topic}',
                    kafka_group_name           = '{group}',
                    kafka_format               = 'JSONEachRow',
                    kafka_num_consumers        = 1,
                    kafka_partition_assignment = 'shard_sticky',
                    kafka_shard_partitions     = '0,1',
                    kafka_replica_consume_mode = 'redundant';

                CREATE TABLE test.events_redundant
                    (key UInt64, src_partition UInt8)
                ENGINE = MergeTree ORDER BY key;

                CREATE MATERIALIZED VIEW test.mv_redundant
                TO test.events_redundant AS
                SELECT key, src_partition FROM test.kafka_redundant;
                """
            )

            for part in range(2):
                _produce_to_partition(
                    kafka_cluster, topic, part,
                    [{"key": part * 10 + i, "src_partition": part} for i in range(5)],
                )

            result = instance.query_with_retry(
                "SELECT count() FROM test.events_redundant",
                timeout=30, sleep_time=1,
                check_callback=lambda r: int(r) == 10,
            )
            assert int(result) == 10, f"Expected 10 rows, got {result}"

        finally:
            instance.query(
                "DROP TABLE IF EXISTS test.mv_redundant SYNC;"
                "DROP TABLE IF EXISTS test.kafka_redundant SYNC;"
                "DROP TABLE IF EXISTS test.events_redundant SYNC;"
            )


# ---------------------------------------------------------------------------
# Test 5: Validation — missing kafka_shard_partitions must raise BAD_ARGUMENTS
# ---------------------------------------------------------------------------

def test_kafka_sticky_partition_requires_shard_partitions(kafka_cluster):
    """
    kafka_partition_assignment='shard_sticky' without kafka_shard_partitions
    must be rejected at table creation time.
    """
    suffix = k.random_string(6)
    with pytest.raises(Exception, match="kafka_shard_partitions"):
        instance.query(
            f"""
            CREATE TABLE test.kafka_bad_{suffix} (key UInt64)
            ENGINE = Kafka
            SETTINGS
                kafka_broker_list          = 'kafka1:19092',
                kafka_topic_list           = 'events',
                kafka_group_name           = 'bad_group',
                kafka_format               = 'JSONEachRow',
                kafka_partition_assignment = 'shard_sticky';
            """
        )


# ---------------------------------------------------------------------------
# Test 6: Validation — invalid kafka_replica_consume_mode must raise BAD_ARGUMENTS
# ---------------------------------------------------------------------------

def test_kafka_sticky_partition_invalid_consume_mode(kafka_cluster):
    """
    An unknown kafka_replica_consume_mode value must raise BAD_ARGUMENTS.
    """
    suffix = k.random_string(6)
    with pytest.raises(Exception, match="kafka_replica_consume_mode"):
        instance.query(
            f"""
            CREATE TABLE test.kafka_bad_mode_{suffix} (key UInt64)
            ENGINE = Kafka
            SETTINGS
                kafka_broker_list          = 'kafka1:19092',
                kafka_topic_list           = 'events',
                kafka_group_name           = 'bad_mode_group',
                kafka_format               = 'JSONEachRow',
                kafka_partition_assignment = 'shard_sticky',
                kafka_shard_partitions     = '0,1',
                kafka_replica_consume_mode = 'invalid_mode';
            """
        )


# ---------------------------------------------------------------------------
# Test 7: Backwards compatibility — empty kafka_partition_assignment = old behaviour
# ---------------------------------------------------------------------------

def test_kafka_sticky_partition_backwards_compatible(kafka_cluster):
    """
    When kafka_partition_assignment is not set (default ''), the table must
    behave exactly like before: broker-managed group assignment across all
    partitions. This ensures the feature is fully opt-in.
    """
    suffix = k.random_string(6)
    topic = f"sticky_compat_{suffix}"
    group = f"sticky_compat_group_{suffix}"

    with k.kafka_topic(k.get_admin_client(kafka_cluster), topic, num_partitions=2):
        try:
            # No kafka_partition_assignment setting — classic subscribe() behaviour
            instance.query(
                f"""
                CREATE TABLE test.kafka_compat (key UInt64)
                ENGINE = Kafka
                SETTINGS
                    kafka_broker_list  = 'kafka1:19092',
                    kafka_topic_list   = '{topic}',
                    kafka_group_name   = '{group}',
                    kafka_format       = 'JSONEachRow',
                    kafka_commit_on_select = 1;
                """
            )

            k.kafka_produce(
                kafka_cluster, topic,
                [json.dumps({"key": i}) for i in range(10)],
            )

            result = instance.query_with_retry(
                f"SELECT count() FROM test.kafka_compat",
                timeout=30, sleep_time=1,
                check_callback=lambda r: int(r) >= 10,
            )
            assert int(result) >= 10, f"Expected >=10 rows in compat mode, got {result}"

        finally:
            instance.query("DROP TABLE IF EXISTS test.kafka_compat SYNC;")
