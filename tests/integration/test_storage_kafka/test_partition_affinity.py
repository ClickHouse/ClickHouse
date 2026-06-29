import json
import time
import logging

import pytest
from kafka import KafkaProducer

from helpers.cluster import ClickHouseCluster
from helpers.client import QueryRuntimeException
from helpers.test_tools import TSV
import helpers.kafka.common as k


cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "instance",
    main_configs=["configs/kafka_and_keeper.xml"],
    user_configs=["configs/users.xml"],
    with_kafka=True,
    with_zookeeper=True,
    macros={
        "kafka_broker": "kafka1",
        "kafka_topic_new": "affinity_topic",
        "kafka_group_name_new": "affinity_group",
        "kafka_client_id": "instance",
        "kafka_format_json_each_row": "JSONEachRow",
    },
)


@pytest.fixture(scope="module")
def kafka_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def kafka_setup_teardown():
    k.clean_test_database_and_topics(instance, cluster)
    yield


def kafka_produce_to_partition(kafka_cluster, topic, partition, messages, retries=15):
    """Produce messages to a specific partition."""
    producer = k.get_kafka_producer(
        kafka_cluster.kafka_port, k.producer_serializer, retries
    )
    for message in messages:
        producer.send(topic=topic, value=message, partition=partition)
    producer.flush()


def test_partition_affinity_two_shards(kafka_cluster):
    """
    Test that kafka_partition_shard_num and kafka_shard_count correctly filter partitions.
    With shard_count=2:
      - shard 0 consumes partitions 0, 2, 4 (partition_id % 2 == 0)
      - shard 1 consumes partitions 1, 3, 5 (partition_id % 2 == 1)
    """
    admin = k.get_admin_client(kafka_cluster)
    topic_name = "affinity_6p_topic"
    num_partitions = 6
    keeper_path_shard0 = "/clickhouse/test/affinity_shard0"
    keeper_path_shard1 = "/clickhouse/test/affinity_shard1"

    k.kafka_create_topic(admin, topic_name, num_partitions=num_partitions)
    with k.existing_kafka_topic(admin, topic_name):
        # Produce messages to each partition with partition-identifying payload
        for p in range(num_partitions):
            msgs = [json.dumps({"partition_id": p, "value": i}) for i in range(3)]
            kafka_produce_to_partition(kafka_cluster, topic_name, p, msgs)

        # Create shard 0 table: consumes partitions 0, 2, 4
        instance.query(
            f"""
            CREATE TABLE test.kafka_shard0 (partition_id UInt64, value UInt64)
            ENGINE = Kafka('{instance.cluster.kafka_host}:19092', '{topic_name}', '{topic_name}_cg_s0', 'JSONEachRow', '\\n')
            SETTINGS kafka_keeper_path = '{keeper_path_shard0}',
                     kafka_replica_name = 'r1',
                     kafka_partition_shard_num = '0',
                     kafka_shard_count = 2
            SETTINGS allow_experimental_kafka_offsets_storage_in_keeper=1;

            CREATE TABLE test.dst_shard0 (partition_id UInt64, value UInt64)
            ENGINE = MergeTree() ORDER BY (partition_id, value);

            CREATE MATERIALIZED VIEW test.mv_shard0 TO test.dst_shard0 AS
            SELECT * FROM test.kafka_shard0;
            """
        )

        # Create shard 1 table: consumes partitions 1, 3, 5
        instance.query(
            f"""
            CREATE TABLE test.kafka_shard1 (partition_id UInt64, value UInt64)
            ENGINE = Kafka('{instance.cluster.kafka_host}:19092', '{topic_name}', '{topic_name}_cg_s1', 'JSONEachRow', '\\n')
            SETTINGS kafka_keeper_path = '{keeper_path_shard1}',
                     kafka_replica_name = 'r1',
                     kafka_partition_shard_num = '1',
                     kafka_shard_count = 2
            SETTINGS allow_experimental_kafka_offsets_storage_in_keeper=1;

            CREATE TABLE test.dst_shard1 (partition_id UInt64, value UInt64)
            ENGINE = MergeTree() ORDER BY (partition_id, value);

            CREATE MATERIALIZED VIEW test.mv_shard1 TO test.dst_shard1 AS
            SELECT * FROM test.kafka_shard1;
            """
        )

        # Wait for shard 0 to consume its 9 messages (3 partitions * 3 messages)
        expected_shard0_count = 9
        expected_shard1_count = 9

        for _ in range(60):
            count0 = int(
                instance.query(
                    "SELECT count() FROM test.dst_shard0 SETTINGS max_execution_time=5"
                ).strip()
            )
            count1 = int(
                instance.query(
                    "SELECT count() FROM test.dst_shard1 SETTINGS max_execution_time=5"
                ).strip()
            )
            if count0 >= expected_shard0_count and count1 >= expected_shard1_count:
                break
            time.sleep(1)

        # Verify shard 0 only has data from partitions 0, 2, 4
        shard0_partitions = instance.query(
            "SELECT DISTINCT partition_id FROM test.dst_shard0 ORDER BY partition_id"
        ).strip()
        assert shard0_partitions == "0\n2\n4", f"Shard 0 got unexpected partitions: {shard0_partitions}"

        # Verify shard 1 only has data from partitions 1, 3, 5
        shard1_partitions = instance.query(
            "SELECT DISTINCT partition_id FROM test.dst_shard1 ORDER BY partition_id"
        ).strip()
        assert shard1_partitions == "1\n3\n5", f"Shard 1 got unexpected partitions: {shard1_partitions}"

        # Verify message counts
        shard0_count = int(
            instance.query("SELECT count() FROM test.dst_shard0").strip()
        )
        shard1_count = int(
            instance.query("SELECT count() FROM test.dst_shard1").strip()
        )
        assert shard0_count == expected_shard0_count, f"Shard 0 count: {shard0_count}"
        assert shard1_count == expected_shard1_count, f"Shard 1 count: {shard1_count}"


def test_partition_affinity_backward_compatible(kafka_cluster):
    """
    Test that without kafka_partition_shard_num/kafka_shard_count, all partitions are consumed
    (backward compatibility).
    """
    admin = k.get_admin_client(kafka_cluster)
    topic_name = "affinity_compat_topic"
    num_partitions = 4
    keeper_path = "/clickhouse/test/affinity_compat"

    k.kafka_create_topic(admin, topic_name, num_partitions=num_partitions)
    with k.existing_kafka_topic(admin, topic_name):
        # Produce messages to each partition
        for p in range(num_partitions):
            msgs = [json.dumps({"partition_id": p, "value": i}) for i in range(2)]
            kafka_produce_to_partition(kafka_cluster, topic_name, p, msgs)

        # Create table without partition affinity settings
        instance.query(
            f"""
            CREATE TABLE test.kafka_all (partition_id UInt64, value UInt64)
            ENGINE = Kafka('{instance.cluster.kafka_host}:19092', '{topic_name}', '{topic_name}_cg', 'JSONEachRow', '\\n')
            SETTINGS kafka_keeper_path = '{keeper_path}',
                     kafka_replica_name = 'r1'
            SETTINGS allow_experimental_kafka_offsets_storage_in_keeper=1;

            CREATE TABLE test.dst_all (partition_id UInt64, value UInt64)
            ENGINE = MergeTree() ORDER BY (partition_id, value);

            CREATE MATERIALIZED VIEW test.mv_all TO test.dst_all AS
            SELECT * FROM test.kafka_all;
            """
        )

        # Wait for all 8 messages (4 partitions * 2 messages)
        expected_count = 8
        for _ in range(60):
            count = int(
                instance.query(
                    "SELECT count() FROM test.dst_all SETTINGS max_execution_time=5"
                ).strip()
            )
            if count >= expected_count:
                break
            time.sleep(1)

        # Verify all partitions are consumed
        all_partitions = instance.query(
            "SELECT DISTINCT partition_id FROM test.dst_all ORDER BY partition_id"
        ).strip()
        assert all_partitions == "0\n1\n2\n3", f"Got unexpected partitions: {all_partitions}"

        total_count = int(
            instance.query("SELECT count() FROM test.dst_all").strip()
        )
        assert total_count == expected_count, f"Total count: {total_count}"


def test_partition_affinity_three_shards(kafka_cluster):
    """
    Test partition affinity with 3 shards and 9 partitions.
    Each shard should get exactly 3 partitions.
    """
    admin = k.get_admin_client(kafka_cluster)
    topic_name = "affinity_9p_topic"
    num_partitions = 9
    shard_count = 3

    k.kafka_create_topic(admin, topic_name, num_partitions=num_partitions)
    with k.existing_kafka_topic(admin, topic_name):
        # Produce 1 message per partition
        for p in range(num_partitions):
            msgs = [json.dumps({"partition_id": p, "value": 1})]
            kafka_produce_to_partition(kafka_cluster, topic_name, p, msgs)

        # Create 3 shard tables
        for shard_num in range(shard_count):
            keeper_path = f"/clickhouse/test/affinity_3s_{shard_num}"
            instance.query(
                f"""
                CREATE TABLE test.kafka_s{shard_num} (partition_id UInt64, value UInt64)
                ENGINE = Kafka('{instance.cluster.kafka_host}:19092', '{topic_name}', '{topic_name}_cg_s{shard_num}', 'JSONEachRow', '\\n')
                SETTINGS kafka_keeper_path = '{keeper_path}',
                         kafka_replica_name = 'r1',
                         kafka_partition_shard_num = '{shard_num}',
                         kafka_shard_count = {shard_count}
                SETTINGS allow_experimental_kafka_offsets_storage_in_keeper=1;

                CREATE TABLE test.dst_s{shard_num} (partition_id UInt64, value UInt64)
                ENGINE = MergeTree() ORDER BY (partition_id, value);

                CREATE MATERIALIZED VIEW test.mv_s{shard_num} TO test.dst_s{shard_num} AS
                SELECT * FROM test.kafka_s{shard_num};
                """
            )

        # Wait for each shard to consume its 3 messages
        for _ in range(60):
            counts = []
            for shard_num in range(shard_count):
                c = int(
                    instance.query(
                        f"SELECT count() FROM test.dst_s{shard_num} SETTINGS max_execution_time=5"
                    ).strip()
                )
                counts.append(c)
            if all(c >= 3 for c in counts):
                break
            time.sleep(1)

        # Verify each shard got the correct partitions
        for shard_num in range(shard_count):
            expected_partitions = sorted(
                [p for p in range(num_partitions) if p % shard_count == shard_num]
            )
            actual_partitions = instance.query(
                f"SELECT DISTINCT partition_id FROM test.dst_s{shard_num} ORDER BY partition_id"
            ).strip()
            expected_str = "\n".join(str(p) for p in expected_partitions)
            assert actual_partitions == expected_str, (
                f"Shard {shard_num}: expected partitions {expected_partitions}, got {actual_partitions}"
            )


def test_partition_affinity_only_partition_num_fails(kafka_cluster):
    """
    Test that specifying kafka_partition_shard_num without kafka_shard_count raises an error.
    """
    with pytest.raises(QueryRuntimeException) as exc_info:
        instance.query(
            f"""
            CREATE TABLE test.kafka_bad1 (value UInt64)
            ENGINE = Kafka('{instance.cluster.kafka_host}:19092', 'some_topic', 'some_group', 'JSONEachRow', '\\n')
            SETTINGS kafka_keeper_path = '/clickhouse/test/bad1',
                     kafka_replica_name = 'r1',
                     kafka_partition_shard_num = '0'
            SETTINGS allow_experimental_kafka_offsets_storage_in_keeper=1;
            """
        )
    assert "must be specified together" in str(exc_info.value)


def test_partition_affinity_only_shard_count_fails(kafka_cluster):
    """
    Test that specifying kafka_shard_count without kafka_partition_shard_num raises an error.
    """
    with pytest.raises(QueryRuntimeException) as exc_info:
        instance.query(
            f"""
            CREATE TABLE test.kafka_bad2 (value UInt64)
            ENGINE = Kafka('{instance.cluster.kafka_host}:19092', 'some_topic', 'some_group', 'JSONEachRow', '\\n')
            SETTINGS kafka_keeper_path = '/clickhouse/test/bad2',
                     kafka_replica_name = 'r1',
                     kafka_shard_count = 2
            SETTINGS allow_experimental_kafka_offsets_storage_in_keeper=1;
            """
        )
    assert "must be specified together" in str(exc_info.value)


def test_partition_affinity_invalid_partition_num_fails(kafka_cluster):
    """
    Test that a non-numeric kafka_partition_shard_num raises an error.
    """
    with pytest.raises(QueryRuntimeException) as exc_info:
        instance.query(
            f"""
            CREATE TABLE test.kafka_bad3 (value UInt64)
            ENGINE = Kafka('{instance.cluster.kafka_host}:19092', 'some_topic', 'some_group', 'JSONEachRow', '\\n')
            SETTINGS kafka_keeper_path = '/clickhouse/test/bad3',
                     kafka_replica_name = 'r1',
                     kafka_partition_shard_num = 'abc',
                     kafka_shard_count = 2
            SETTINGS allow_experimental_kafka_offsets_storage_in_keeper=1;
            """
        )
    assert "must be a valid non-negative integer" in str(exc_info.value)


def test_partition_affinity_fewer_partitions_than_shards(kafka_cluster):
    """
    Test that when there are fewer partitions than shards, extra shards simply
    don't consume anything (no error).
    E.g. 2 partitions with 3 shards: shard 0 gets partition 0, shard 1 gets partition 1,
    shard 2 gets nothing.
    """
    admin = k.get_admin_client(kafka_cluster)
    topic_name = "affinity_2p_3s_topic"
    num_partitions = 2
    shard_count = 3
    keeper_path_base = "/clickhouse/test/affinity_fewer"

    k.kafka_create_topic(admin, topic_name, num_partitions=num_partitions)
    with k.existing_kafka_topic(admin, topic_name):
        # Produce messages to each partition
        for p in range(num_partitions):
            msgs = [json.dumps({"partition_id": p, "value": i}) for i in range(3)]
            kafka_produce_to_partition(kafka_cluster, topic_name, p, msgs)

        # Create 3 shard tables
        for shard_num in range(shard_count):
            keeper_path = f"{keeper_path_base}_{shard_num}"
            instance.query(
                f"""
                CREATE TABLE test.kafka_fp_s{shard_num} (partition_id UInt64, value UInt64)
                ENGINE = Kafka('{instance.cluster.kafka_host}:19092', '{topic_name}', '{topic_name}_cg_s{shard_num}', 'JSONEachRow', '\\n')
                SETTINGS kafka_keeper_path = '{keeper_path}',
                         kafka_replica_name = 'r1',
                         kafka_partition_shard_num = '{shard_num}',
                         kafka_shard_count = {shard_count}
                SETTINGS allow_experimental_kafka_offsets_storage_in_keeper=1;

                CREATE TABLE test.dst_fp_s{shard_num} (partition_id UInt64, value UInt64)
                ENGINE = MergeTree() ORDER BY (partition_id, value);

                CREATE MATERIALIZED VIEW test.mv_fp_s{shard_num} TO test.dst_fp_s{shard_num} AS
                SELECT * FROM test.kafka_fp_s{shard_num};
                """
            )

        # Wait for shard 0 and shard 1 to consume their messages
        for _ in range(60):
            count0 = int(
                instance.query(
                    "SELECT count() FROM test.dst_fp_s0 SETTINGS max_execution_time=5"
                ).strip()
            )
            count1 = int(
                instance.query(
                    "SELECT count() FROM test.dst_fp_s1 SETTINGS max_execution_time=5"
                ).strip()
            )
            if count0 >= 3 and count1 >= 3:
                break
            time.sleep(1)

        # Shard 0 should have partition 0 only
        s0_partitions = instance.query(
            "SELECT DISTINCT partition_id FROM test.dst_fp_s0 ORDER BY partition_id"
        ).strip()
        assert s0_partitions == "0", f"Shard 0 got unexpected partitions: {s0_partitions}"

        # Shard 1 should have partition 1 only
        s1_partitions = instance.query(
            "SELECT DISTINCT partition_id FROM test.dst_fp_s1 ORDER BY partition_id"
        ).strip()
        assert s1_partitions == "1", f"Shard 1 got unexpected partitions: {s1_partitions}"

        # Shard 2 should have no data (no partition_id % 3 == 2 exists)
        s2_count = int(
            instance.query("SELECT count() FROM test.dst_fp_s2").strip()
        )
        assert s2_count == 0, f"Shard 2 should have no data, got {s2_count} rows"


def test_partition_affinity_replica_failover(kafka_cluster):
    """
    Test that DETACH/ATTACH of a Kafka table does not break affinity boundaries.

    Setup: 6 partitions, shard_count=2, so shard 0 owns partitions {0, 2, 4}.
    A single replica consumes shard 0. After DETACH and ATTACH, it should resume
    consuming only the eligible partitions without violating affinity.
    Meanwhile, shard 1 (partitions {1, 3, 5}) is consumed by a separate table
    to verify cross-shard isolation during failover.
    """
    admin = k.get_admin_client(kafka_cluster)
    topic_name = "affinity_failover_topic"
    num_partitions = 6
    keeper_path_s0 = "/clickhouse/test/affinity_failover_s0"
    keeper_path_s1 = "/clickhouse/test/affinity_failover_s1"

    k.kafka_create_topic(admin, topic_name, num_partitions=num_partitions)
    with k.existing_kafka_topic(admin, topic_name):
        # Produce initial batch: 5 messages per partition
        for p in range(num_partitions):
            msgs = [json.dumps({"partition_id": p, "value": i}) for i in range(5)]
            kafka_produce_to_partition(kafka_cluster, topic_name, p, msgs)

        # Create shard 0 table (partitions 0, 2, 4) and shard 1 table (partitions 1, 3, 5)
        instance.query(
            f"""
            CREATE TABLE test.kafka_fo_s0 (partition_id UInt64, value UInt64)
            ENGINE = Kafka('{instance.cluster.kafka_host}:19092', '{topic_name}', '{topic_name}_cg_s0', 'JSONEachRow', '\\n')
            SETTINGS kafka_keeper_path = '{keeper_path_s0}',
                     kafka_replica_name = 'r1',
                     kafka_partition_shard_num = '0',
                     kafka_shard_count = 2
            SETTINGS allow_experimental_kafka_offsets_storage_in_keeper=1;

            CREATE TABLE test.dst_fo_s0 (partition_id UInt64, value UInt64)
            ENGINE = MergeTree() ORDER BY (partition_id, value);

            CREATE MATERIALIZED VIEW test.mv_fo_s0 TO test.dst_fo_s0 AS
            SELECT * FROM test.kafka_fo_s0;

            CREATE TABLE test.kafka_fo_s1 (partition_id UInt64, value UInt64)
            ENGINE = Kafka('{instance.cluster.kafka_host}:19092', '{topic_name}', '{topic_name}_cg_s1', 'JSONEachRow', '\\n')
            SETTINGS kafka_keeper_path = '{keeper_path_s1}',
                     kafka_replica_name = 'r1',
                     kafka_partition_shard_num = '1',
                     kafka_shard_count = 2
            SETTINGS allow_experimental_kafka_offsets_storage_in_keeper=1;

            CREATE TABLE test.dst_fo_s1 (partition_id UInt64, value UInt64)
            ENGINE = MergeTree() ORDER BY (partition_id, value);

            CREATE MATERIALIZED VIEW test.mv_fo_s1 TO test.dst_fo_s1 AS
            SELECT * FROM test.kafka_fo_s1;
            """
        )

        # Wait for initial messages: shard 0 gets 15, shard 1 gets 15
        for _ in range(60):
            count_s0 = int(instance.query("SELECT count() FROM test.dst_fo_s0").strip())
            count_s1 = int(instance.query("SELECT count() FROM test.dst_fo_s1").strip())
            if count_s0 >= 15 and count_s1 >= 15:
                break
            time.sleep(1)

        count_s0 = int(instance.query("SELECT count() FROM test.dst_fo_s0").strip())
        count_s1 = int(instance.query("SELECT count() FROM test.dst_fo_s1").strip())
        assert count_s0 >= 15, f"Shard 0 expected 15 messages, got {count_s0}"
        assert count_s1 >= 15, f"Shard 1 expected 15 messages, got {count_s1}"

        # Verify affinity before detach
        s0_partitions = instance.query(
            "SELECT DISTINCT partition_id FROM test.dst_fo_s0 ORDER BY partition_id"
        ).strip()
        assert s0_partitions == "0\n2\n4", f"Shard 0 affinity violated before detach: {s0_partitions}"

        s1_partitions = instance.query(
            "SELECT DISTINCT partition_id FROM test.dst_fo_s1 ORDER BY partition_id"
        ).strip()
        assert s1_partitions == "1\n3\n5", f"Shard 1 affinity violated before detach: {s1_partitions}"

        # Detach shard 0 to simulate failure
        instance.query("DETACH TABLE test.kafka_fo_s0")

        # Produce more messages to all partitions while shard 0 is down
        for p in range(num_partitions):
            msgs = [json.dumps({"partition_id": p, "value": 100 + i}) for i in range(5)]
            kafka_produce_to_partition(kafka_cluster, topic_name, p, msgs)

        # Shard 1 should still consume its new messages (partitions 1, 3, 5)
        for _ in range(60):
            count_s1 = int(instance.query("SELECT count() FROM test.dst_fo_s1").strip())
            if count_s1 >= 30:
                break
            time.sleep(1)

        count_s1 = int(instance.query("SELECT count() FROM test.dst_fo_s1").strip())
        assert count_s1 >= 30, f"Shard 1 should have consumed new messages while shard 0 is down, got {count_s1}"

        # Shard 0 dst should still have only the original 15 (frozen)
        count_s0_frozen = int(instance.query("SELECT count() FROM test.dst_fo_s0").strip())
        assert count_s0_frozen == 15, f"Shard 0 dst should be frozen at 15, got {count_s0_frozen}"

        # Re-attach shard 0
        instance.query("ATTACH TABLE test.kafka_fo_s0")

        # Wait for shard 0 to consume the messages produced while it was down
        for _ in range(60):
            count_s0 = int(instance.query("SELECT count() FROM test.dst_fo_s0").strip())
            if count_s0 >= 30:
                break
            time.sleep(1)

        count_s0 = int(instance.query("SELECT count() FROM test.dst_fo_s0").strip())
        assert count_s0 >= 30, \
            f"Shard 0 should have resumed and consumed new messages after attach, got {count_s0}"

        # Verify affinity is still respected after ATTACH
        s0_partitions_after = instance.query(
            "SELECT DISTINCT partition_id FROM test.dst_fo_s0 ORDER BY partition_id"
        ).strip()
        assert s0_partitions_after == "0\n2\n4", \
            f"Shard 0 affinity violated after re-attach: {s0_partitions_after}"

        s1_partitions_after = instance.query(
            "SELECT DISTINCT partition_id FROM test.dst_fo_s1 ORDER BY partition_id"
        ).strip()
        assert s1_partitions_after == "1\n3\n5", \
            f"Shard 1 affinity violated after shard 0 re-attach: {s1_partitions_after}"

        # Verify no cross-shard leakage
        leaked_s0 = instance.query(
            "SELECT count() FROM test.dst_fo_s0 WHERE partition_id % 2 != 0"
        ).strip()
        assert leaked_s0 == "0", f"Shard 0 leaked {leaked_s0} messages from odd partitions"

        leaked_s1 = instance.query(
            "SELECT count() FROM test.dst_fo_s1 WHERE partition_id % 2 != 1"
        ).strip()
        assert leaked_s1 == "0", f"Shard 1 leaked {leaked_s1} messages from even partitions"


if __name__ == "__main__":
    cluster.start()
    input("Cluster created, press any key to destroy...")
    cluster.shutdown()
