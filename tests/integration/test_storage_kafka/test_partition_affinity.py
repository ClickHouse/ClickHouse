import json
import time

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.client import QueryRuntimeException
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
        "kafka_shard_num_bad": "3",
        "kafka_shard_num_empty": "",
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


def create_affinity_shard(instance, topic_name, shard_num, shard_count, keeper_path, table_suffix):
    """Create a Kafka table with affinity settings, a destination MergeTree table, and a MV."""
    instance.query(
        f"""
        CREATE TABLE test.kafka_{table_suffix} (partition_id UInt64, value UInt64)
        ENGINE = Kafka('{instance.cluster.kafka_host}:19092', '{topic_name}', '{topic_name}_cg_s{shard_num}', 'JSONEachRow', '\\n')
        SETTINGS kafka_keeper_path = '{keeper_path}',
                 kafka_replica_name = 'r1',
                 kafka_partition_shard_num = '{shard_num}',
                 kafka_shard_count = {shard_count}
        SETTINGS allow_experimental_kafka_offsets_storage_in_keeper=1;

        CREATE TABLE test.dst_{table_suffix} (partition_id UInt64, value UInt64)
        ENGINE = MergeTree() ORDER BY (partition_id, value);

        CREATE MATERIALIZED VIEW test.mv_{table_suffix} TO test.dst_{table_suffix} AS
        SELECT * FROM test.kafka_{table_suffix};
        """
    )


def wait_for_count(instance, table, expected, timeout=60):
    """Wait until the table has at least `expected` rows."""
    for _ in range(timeout):
        count = int(instance.query(f"SELECT count() FROM {table} SETTINGS max_execution_time=5").strip())
        if count >= expected:
            return count
        time.sleep(1)
    return int(instance.query(f"SELECT count() FROM {table}").strip())


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

    k.kafka_create_topic(admin, topic_name, num_partitions=num_partitions)
    with k.existing_kafka_topic(admin, topic_name):
        for p in range(num_partitions):
            msgs = [json.dumps({"partition_id": p, "value": i}) for i in range(3)]
            kafka_produce_to_partition(kafka_cluster, topic_name, p, msgs)

        create_affinity_shard(instance, topic_name, 0, 2, "/clickhouse/test/affinity_shard0", "shard0")
        create_affinity_shard(instance, topic_name, 1, 2, "/clickhouse/test/affinity_shard1", "shard1")

        # Wait for both shards to consume their 9 messages (3 partitions * 3 messages)
        wait_for_count(instance, "test.dst_shard0", 9)
        wait_for_count(instance, "test.dst_shard1", 9)

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
        shard0_count = int(instance.query("SELECT count() FROM test.dst_shard0").strip())
        shard1_count = int(instance.query("SELECT count() FROM test.dst_shard1").strip())
        assert shard0_count == 9, f"Shard 0 count: {shard0_count}"
        assert shard1_count == 9, f"Shard 1 count: {shard1_count}"


def test_partition_affinity_backward_compatible(kafka_cluster):
    """
    Test that without kafka_partition_shard_num/kafka_shard_count, all partitions are consumed.
    """
    admin = k.get_admin_client(kafka_cluster)
    topic_name = "affinity_compat_topic"
    num_partitions = 4
    keeper_path = "/clickhouse/test/affinity_compat"

    k.kafka_create_topic(admin, topic_name, num_partitions=num_partitions)
    with k.existing_kafka_topic(admin, topic_name):
        for p in range(num_partitions):
            msgs = [json.dumps({"partition_id": p, "value": i}) for i in range(2)]
            kafka_produce_to_partition(kafka_cluster, topic_name, p, msgs)

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

        wait_for_count(instance, "test.dst_all", 8)

        all_partitions = instance.query(
            "SELECT DISTINCT partition_id FROM test.dst_all ORDER BY partition_id"
        ).strip()
        assert all_partitions == "0\n1\n2\n3", f"Got unexpected partitions: {all_partitions}"

        total_count = int(instance.query("SELECT count() FROM test.dst_all").strip())
        assert total_count == 8, f"Total count: {total_count}"


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
        for p in range(num_partitions):
            msgs = [json.dumps({"partition_id": p, "value": 1})]
            kafka_produce_to_partition(kafka_cluster, topic_name, p, msgs)

        for shard_num in range(shard_count):
            create_affinity_shard(
                instance, topic_name, shard_num, shard_count,
                f"/clickhouse/test/affinity_3s_{shard_num}", f"s{shard_num}")

        # Wait for each shard to consume its 3 messages
        for shard_num in range(shard_count):
            wait_for_count(instance, f"test.dst_s{shard_num}", 3)

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


def test_partition_affinity_macro_expanded_out_of_range_fails(kafka_cluster):
    """
    Regression test: when kafka_partition_shard_num uses a macro that expands
    to a value > kafka_shard_count, CREATE TABLE must fail with BAD_ARGUMENTS.
    The macro {kafka_shard_num_bad} expands to '3', which is greater than shard_count=2.
    """
    with pytest.raises(QueryRuntimeException) as exc_info:
        instance.query(
            f"""
            CREATE TABLE test.kafka_bad_macro (value UInt64)
            ENGINE = Kafka('{instance.cluster.kafka_host}:19092', 'some_topic', 'some_group', 'JSONEachRow', '\\n')
            SETTINGS kafka_keeper_path = '/clickhouse/test/bad_macro',
                     kafka_replica_name = 'r1',
                     kafka_partition_shard_num = '{{kafka_shard_num_bad}}',
                     kafka_shard_count = 2
            SETTINGS allow_experimental_kafka_offsets_storage_in_keeper=1;
            """
        )
    assert "must not be greater than" in str(exc_info.value)


def test_partition_affinity_macro_expanded_to_empty_fails(kafka_cluster):
    """
    Regression test: when kafka_partition_shard_num uses a macro that expands
    to an empty string, CREATE TABLE must fail with BAD_ARGUMENTS instead of
    silently disabling partition affinity.
    The macro {kafka_shard_num_empty} expands to '', which is invalid.
    """
    with pytest.raises(QueryRuntimeException) as exc_info:
        instance.query(
            f"""
            CREATE TABLE test.kafka_bad_empty_macro (value UInt64)
            ENGINE = Kafka('{instance.cluster.kafka_host}:19092', 'some_topic', 'some_group', 'JSONEachRow', '\\n')
            SETTINGS kafka_keeper_path = '/clickhouse/test/bad_empty_macro',
                     kafka_replica_name = 'r1',
                     kafka_partition_shard_num = '{{kafka_shard_num_empty}}',
                     kafka_shard_count = 2
            SETTINGS allow_experimental_kafka_offsets_storage_in_keeper=1;
            """
        )
    assert "expanded to an empty string" in str(exc_info.value)


def test_partition_affinity_equality_1based_succeeds(kafka_cluster):
    """
    Positive test: kafka_partition_shard_num == kafka_shard_count is allowed
    to support 1-based shard numbering. shard_num=2, shard_count=2 maps to
    effective shard 2%2=0, consuming partitions 0,2.
    """
    admin = k.get_admin_client(kafka_cluster)
    topic_name = "affinity_1based_topic"
    num_partitions = 4

    k.kafka_create_topic(admin, topic_name, num_partitions=num_partitions)
    with k.existing_kafka_topic(admin, topic_name):
        for p in range(num_partitions):
            msgs = [json.dumps({"partition_id": p, "value": i}) for i in range(2)]
            kafka_produce_to_partition(kafka_cluster, topic_name, p, msgs)

        # shard_num=2 with shard_count=2: effective shard = 2%2 = 0
        create_affinity_shard(
            instance, topic_name, 2, 2,
            "/clickhouse/test/affinity_1based", "1based")

        wait_for_count(instance, "test.dst_1based", 4)

        partitions = instance.query(
            "SELECT DISTINCT partition_id FROM test.dst_1based ORDER BY partition_id"
        ).strip()
        assert partitions == "0\n2", f"1-based shard got unexpected partitions: {partitions}"

        total_count = int(instance.query("SELECT count() FROM test.dst_1based").strip())
        assert total_count == 4, f"Expected 4, got {total_count}"


def test_partition_affinity_fewer_partitions_than_shards(kafka_cluster):
    """
    Test that when there are fewer partitions than shards, extra shards simply
    don't consume anything (no error).
    """
    admin = k.get_admin_client(kafka_cluster)
    topic_name = "affinity_2p_3s_topic"
    num_partitions = 2
    shard_count = 3

    k.kafka_create_topic(admin, topic_name, num_partitions=num_partitions)
    with k.existing_kafka_topic(admin, topic_name):
        for p in range(num_partitions):
            msgs = [json.dumps({"partition_id": p, "value": i}) for i in range(3)]
            kafka_produce_to_partition(kafka_cluster, topic_name, p, msgs)

        for shard_num in range(shard_count):
            create_affinity_shard(
                instance, topic_name, shard_num, shard_count,
                f"/clickhouse/test/affinity_fewer_{shard_num}", f"fp_s{shard_num}")

        wait_for_count(instance, "test.dst_fp_s0", 3)
        wait_for_count(instance, "test.dst_fp_s1", 3)

        s0_partitions = instance.query(
            "SELECT DISTINCT partition_id FROM test.dst_fp_s0 ORDER BY partition_id"
        ).strip()
        assert s0_partitions == "0", f"Shard 0 got unexpected partitions: {s0_partitions}"

        s1_partitions = instance.query(
            "SELECT DISTINCT partition_id FROM test.dst_fp_s1 ORDER BY partition_id"
        ).strip()
        assert s1_partitions == "1", f"Shard 1 got unexpected partitions: {s1_partitions}"

        # Shard 2 should have no data (no partition_id % 3 == 2 exists)
        s2_count = int(instance.query("SELECT count() FROM test.dst_fp_s2").strip())
        assert s2_count == 0, f"Shard 2 should have no data, got {s2_count} rows"


def test_partition_affinity_replica_failover(kafka_cluster):
    """
    Test that DETACH/ATTACH of a Kafka table does not break affinity boundaries.
    Setup: 6 partitions, shard_count=2, so shard 0 owns partitions {0, 2, 4}.
    """
    admin = k.get_admin_client(kafka_cluster)
    topic_name = "affinity_failover_topic"
    num_partitions = 6

    k.kafka_create_topic(admin, topic_name, num_partitions=num_partitions)
    with k.existing_kafka_topic(admin, topic_name):
        # Produce initial batch: 5 messages per partition
        for p in range(num_partitions):
            msgs = [json.dumps({"partition_id": p, "value": i}) for i in range(5)]
            kafka_produce_to_partition(kafka_cluster, topic_name, p, msgs)

        create_affinity_shard(
            instance, topic_name, 0, 2,
            "/clickhouse/test/affinity_failover_s0", "fo_s0")
        create_affinity_shard(
            instance, topic_name, 1, 2,
            "/clickhouse/test/affinity_failover_s1", "fo_s1")

        # Wait for initial messages: shard 0 gets 15, shard 1 gets 15
        wait_for_count(instance, "test.dst_fo_s0", 15)
        wait_for_count(instance, "test.dst_fo_s1", 15)

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

        # Produce more messages while shard 0 is down
        for p in range(num_partitions):
            msgs = [json.dumps({"partition_id": p, "value": 100 + i}) for i in range(5)]
            kafka_produce_to_partition(kafka_cluster, topic_name, p, msgs)

        # Shard 1 should still consume its new messages
        wait_for_count(instance, "test.dst_fo_s1", 30)

        # Shard 0 dst should still have only the original 15 (frozen)
        count_s0_frozen = int(instance.query("SELECT count() FROM test.dst_fo_s0").strip())
        assert count_s0_frozen == 15, f"Shard 0 dst should be frozen at 15, got {count_s0_frozen}"

        # Re-attach shard 0
        instance.query("ATTACH TABLE test.kafka_fo_s0")

        # Wait for shard 0 to consume the messages produced while it was down
        wait_for_count(instance, "test.dst_fo_s0", 30)

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
