import json

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
    instance.query_with_retry(
        f"SELECT count() FROM {table}",
        check_callback=lambda result: int(result.strip()) >= expected,
        retry_count=timeout,
        sleep_time=1,
    )


def verify_shard_partitions(instance, table_suffix, shard_num, shard_count, num_partitions):
    """Verify a shard consumed only its expected partitions with no leakage."""
    expected_partitions = sorted(
        [p for p in range(num_partitions) if p % shard_count == shard_num]
    )
    actual_partitions = instance.query(
        f"SELECT DISTINCT partition_id FROM test.dst_{table_suffix} ORDER BY partition_id"
    ).strip()
    expected_str = "\n".join(str(p) for p in expected_partitions)
    assert actual_partitions == expected_str, (
        f"Shard {shard_num}: expected partitions {expected_partitions}, got {actual_partitions}"
    )
    leaked = instance.query(
        f"SELECT count() FROM test.dst_{table_suffix} WHERE partition_id % {shard_count} != {shard_num}"
    ).strip()
    assert leaked == "0", f"Shard {shard_num} leaked {leaked} messages"


def test_partition_affinity_basic(kafka_cluster):
    """
    Test basic partition affinity with shared keeper_path and 2 shards.
    6 partitions, 2 shards, all using the SAME keeper_path (isolation via path suffix).
      - shard 0: partitions 0, 2, 4
      - shard 1: partitions 1, 3, 5
    """
    admin = k.get_admin_client(kafka_cluster)
    topic_name = "affinity_basic_topic"
    num_partitions = 6
    shard_count = 2
    shared_keeper_path = "/clickhouse/test/affinity_basic"

    k.kafka_create_topic(admin, topic_name, num_partitions=num_partitions)
    with k.existing_kafka_topic(admin, topic_name):
        for p in range(num_partitions):
            msgs = [json.dumps({"partition_id": p, "value": i}) for i in range(3)]
            kafka_produce_to_partition(kafka_cluster, topic_name, p, msgs)

        for shard_num in range(shard_count):
            create_affinity_shard(
                instance, topic_name, shard_num, shard_count,
                shared_keeper_path, f"basic_s{shard_num}")

        # Each shard: 3 partitions * 3 msgs = 9
        wait_for_count(instance, "test.dst_basic_s0", 9)
        wait_for_count(instance, "test.dst_basic_s1", 9)

        for shard_num in range(shard_count):
            verify_shard_partitions(instance, f"basic_s{shard_num}", shard_num, shard_count, num_partitions)

        total = sum(
            int(instance.query(f"SELECT count() FROM test.dst_basic_s{s}").strip())
            for s in range(shard_count)
        )
        assert total == 18, f"Expected 18 total messages, got {total}"


def test_partition_affinity_backward_compatible(kafka_cluster):
    """
    Without kafka_partition_shard_num/kafka_shard_count, all partitions are consumed normally.
    """
    admin = k.get_admin_client(kafka_cluster)
    topic_name = "affinity_compat_topic"
    num_partitions = 4

    k.kafka_create_topic(admin, topic_name, num_partitions=num_partitions)
    with k.existing_kafka_topic(admin, topic_name):
        for p in range(num_partitions):
            msgs = [json.dumps({"partition_id": p, "value": i}) for i in range(2)]
            kafka_produce_to_partition(kafka_cluster, topic_name, p, msgs)

        instance.query(
            f"""
            CREATE TABLE test.kafka_all (partition_id UInt64, value UInt64)
            ENGINE = Kafka('{instance.cluster.kafka_host}:19092', '{topic_name}', '{topic_name}_cg', 'JSONEachRow', '\\n')
            SETTINGS kafka_keeper_path = '/clickhouse/test/affinity_compat',
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


def test_partition_affinity_settings_validation(kafka_cluster):
    """
    Validate all error conditions for affinity settings in one test:
    - only kafka_partition_shard_num without kafka_shard_count
    - only kafka_shard_count without kafka_partition_shard_num
    - non-numeric kafka_partition_shard_num
    - macro expanding to value > kafka_shard_count
    - macro expanding to empty string
    """
    host = f"{instance.cluster.kafka_host}:19092"

    # Only kafka_partition_shard_num without kafka_shard_count
    with pytest.raises(QueryRuntimeException) as exc_info:
        instance.query(
            f"""
            CREATE TABLE test.kafka_bad1 (value UInt64)
            ENGINE = Kafka('{host}', 'some_topic', 'some_group', 'JSONEachRow', '\\n')
            SETTINGS kafka_keeper_path = '/clickhouse/test/bad1',
                     kafka_replica_name = 'r1',
                     kafka_partition_shard_num = '0'
            SETTINGS allow_experimental_kafka_offsets_storage_in_keeper=1;
            """
        )
    assert "must be specified together" in str(exc_info.value)

    # Only kafka_shard_count without kafka_partition_shard_num
    with pytest.raises(QueryRuntimeException) as exc_info:
        instance.query(
            f"""
            CREATE TABLE test.kafka_bad2 (value UInt64)
            ENGINE = Kafka('{host}', 'some_topic', 'some_group', 'JSONEachRow', '\\n')
            SETTINGS kafka_keeper_path = '/clickhouse/test/bad2',
                     kafka_replica_name = 'r1',
                     kafka_shard_count = 2
            SETTINGS allow_experimental_kafka_offsets_storage_in_keeper=1;
            """
        )
    assert "must be specified together" in str(exc_info.value)

    # Non-numeric kafka_partition_shard_num
    with pytest.raises(QueryRuntimeException) as exc_info:
        instance.query(
            f"""
            CREATE TABLE test.kafka_bad3 (value UInt64)
            ENGINE = Kafka('{host}', 'some_topic', 'some_group', 'JSONEachRow', '\\n')
            SETTINGS kafka_keeper_path = '/clickhouse/test/bad3',
                     kafka_replica_name = 'r1',
                     kafka_partition_shard_num = 'abc',
                     kafka_shard_count = 2
            SETTINGS allow_experimental_kafka_offsets_storage_in_keeper=1;
            """
        )
    assert "must be a valid non-negative integer" in str(exc_info.value)

    # Macro expanding to value > kafka_shard_count
    with pytest.raises(QueryRuntimeException) as exc_info:
        instance.query(
            f"""
            CREATE TABLE test.kafka_bad4 (value UInt64)
            ENGINE = Kafka('{host}', 'some_topic', 'some_group', 'JSONEachRow', '\\n')
            SETTINGS kafka_keeper_path = '/clickhouse/test/bad4',
                     kafka_replica_name = 'r1',
                     kafka_partition_shard_num = '{{kafka_shard_num_bad}}',
                     kafka_shard_count = 2
            SETTINGS allow_experimental_kafka_offsets_storage_in_keeper=1;
            """
        )
    assert "must not be greater than" in str(exc_info.value)

    # Macro expanding to empty string
    with pytest.raises(QueryRuntimeException) as exc_info:
        instance.query(
            f"""
            CREATE TABLE test.kafka_bad5 (value UInt64)
            ENGINE = Kafka('{host}', 'some_topic', 'some_group', 'JSONEachRow', '\\n')
            SETTINGS kafka_keeper_path = '/clickhouse/test/bad5',
                     kafka_replica_name = 'r1',
                     kafka_partition_shard_num = '{{kafka_shard_num_empty}}',
                     kafka_shard_count = 2
            SETTINGS allow_experimental_kafka_offsets_storage_in_keeper=1;
            """
        )
    assert "expanded to an empty string" in str(exc_info.value)


def test_partition_affinity_edge_cases(kafka_cluster):
    """
    Edge cases:
    1. kafka_partition_shard_num == kafka_shard_count (1-based numbering): shard_num=2, shard_count=2
       maps to effective shard 0, consuming partitions 0,2.
    2. Fewer partitions than shards: 2 partitions, 3 shards. Shard 2 gets nothing.
    """
    admin = k.get_admin_client(kafka_cluster)

    # Case 1: 1-based shard numbering
    topic1 = "affinity_1based_topic"
    k.kafka_create_topic(admin, topic1, num_partitions=4)
    with k.existing_kafka_topic(admin, topic1):
        for p in range(4):
            msgs = [json.dumps({"partition_id": p, "value": i}) for i in range(2)]
            kafka_produce_to_partition(kafka_cluster, topic1, p, msgs)

        # shard_num=2 with shard_count=2: effective shard = 2%2 = 0
        create_affinity_shard(
            instance, topic1, 2, 2,
            "/clickhouse/test/affinity_1based", "1based")

        wait_for_count(instance, "test.dst_1based", 4)

        partitions = instance.query(
            "SELECT DISTINCT partition_id FROM test.dst_1based ORDER BY partition_id"
        ).strip()
        assert partitions == "0\n2", f"1-based shard got unexpected partitions: {partitions}"

    k.clean_test_database_and_topics(instance, cluster)

    # Case 2: fewer partitions than shards
    topic2 = "affinity_fewer_topic"
    k.kafka_create_topic(admin, topic2, num_partitions=2)
    with k.existing_kafka_topic(admin, topic2):
        for p in range(2):
            msgs = [json.dumps({"partition_id": p, "value": i}) for i in range(3)]
            kafka_produce_to_partition(kafka_cluster, topic2, p, msgs)

        shared_path = "/clickhouse/test/affinity_fewer"
        for shard_num in range(3):
            create_affinity_shard(
                instance, topic2, shard_num, 3, shared_path, f"fewer_s{shard_num}")

        wait_for_count(instance, "test.dst_fewer_s0", 3)
        wait_for_count(instance, "test.dst_fewer_s1", 3)

        s0 = instance.query(
            "SELECT DISTINCT partition_id FROM test.dst_fewer_s0 ORDER BY partition_id"
        ).strip()
        assert s0 == "0", f"Shard 0 got: {s0}"

        s1 = instance.query(
            "SELECT DISTINCT partition_id FROM test.dst_fewer_s1 ORDER BY partition_id"
        ).strip()
        assert s1 == "1", f"Shard 1 got: {s1}"

        # Shard 2 should have no data
        s2_count = int(instance.query("SELECT count() FROM test.dst_fewer_s2").strip())
        assert s2_count == 0, f"Shard 2 should have no data, got {s2_count} rows"


def test_partition_affinity_uneven_distribution(kafka_cluster):
    """
    Test partition affinity when num_partitions is not evenly divisible by shard_count.
    With 7 partitions and 3 shards:
      - shard 0: partitions 0, 3, 6 (3 partitions)
      - shard 1: partitions 1, 4    (2 partitions)
      - shard 2: partitions 2, 5    (2 partitions)
    Some shards get more partitions than others, but no errors should occur.
    """
    admin = k.get_admin_client(kafka_cluster)
    topic_name = "affinity_uneven_topic"
    num_partitions = 7
    shard_count = 3
    shared_keeper_path = "/clickhouse/test/affinity_uneven"

    k.kafka_create_topic(admin, topic_name, num_partitions=num_partitions)
    with k.existing_kafka_topic(admin, topic_name):
        for p in range(num_partitions):
            msgs = [json.dumps({"partition_id": p, "value": i}) for i in range(2)]
            kafka_produce_to_partition(kafka_cluster, topic_name, p, msgs)

        for shard_num in range(shard_count):
            create_affinity_shard(
                instance, topic_name, shard_num, shard_count,
                shared_keeper_path, f"uneven_s{shard_num}")

        # shard 0: partitions 0,3,6 -> 6 messages
        # shard 1: partitions 1,4   -> 4 messages
        # shard 2: partitions 2,5   -> 4 messages
        wait_for_count(instance, "test.dst_uneven_s0", 6)
        wait_for_count(instance, "test.dst_uneven_s1", 4)
        wait_for_count(instance, "test.dst_uneven_s2", 4)

        for shard_num in range(shard_count):
            verify_shard_partitions(instance, f"uneven_s{shard_num}", shard_num, shard_count, num_partitions)

        total = sum(
            int(instance.query(f"SELECT count() FROM test.dst_uneven_s{s}").strip())
            for s in range(shard_count)
        )
        assert total == 14, f"Expected 14 total messages, got {total}"


def test_partition_affinity_single_replica_failover(kafka_cluster):
    """
    DETACH/ATTACH of a shard does not break affinity boundaries.
    After re-attach, the shard resumes consuming only its own partitions.
    """
    admin = k.get_admin_client(kafka_cluster)
    topic_name = "affinity_failover_topic"
    num_partitions = 6
    shard_count = 2
    shared_keeper_path = "/clickhouse/test/affinity_failover"

    k.kafka_create_topic(admin, topic_name, num_partitions=num_partitions)
    with k.existing_kafka_topic(admin, topic_name):
        for p in range(num_partitions):
            msgs = [json.dumps({"partition_id": p, "value": i}) for i in range(5)]
            kafka_produce_to_partition(kafka_cluster, topic_name, p, msgs)

        create_affinity_shard(instance, topic_name, 0, shard_count, shared_keeper_path, "fo_s0")
        create_affinity_shard(instance, topic_name, 1, shard_count, shared_keeper_path, "fo_s1")

        wait_for_count(instance, "test.dst_fo_s0", 15)
        wait_for_count(instance, "test.dst_fo_s1", 15)

        # Detach shard 0
        instance.query("DETACH TABLE test.kafka_fo_s0")

        # Produce more while shard 0 is down
        for p in range(num_partitions):
            msgs = [json.dumps({"partition_id": p, "value": 100 + i}) for i in range(5)]
            kafka_produce_to_partition(kafka_cluster, topic_name, p, msgs)

        # Shard 1 continues consuming its partitions
        wait_for_count(instance, "test.dst_fo_s1", 30)

        # Shard 0 frozen
        count_s0 = int(instance.query("SELECT count() FROM test.dst_fo_s0").strip())
        assert count_s0 == 15, f"Shard 0 should be frozen at 15, got {count_s0}"

        # Re-attach shard 0
        instance.query("ATTACH TABLE test.kafka_fo_s0")
        wait_for_count(instance, "test.dst_fo_s0", 30)

        # Verify affinity preserved after re-attach
        verify_shard_partitions(instance, "fo_s0", 0, shard_count, num_partitions)
        verify_shard_partitions(instance, "fo_s1", 1, shard_count, num_partitions)


def test_partition_affinity_multi_replica_failover(kafka_cluster):
    """
    Multiple replicas (different kafka_replica_name) for the same kafka_partition_shard_num
    share the same Keeper coordination path. When one replica is detached, the surviving
    replica must reclaim ALL partitions belonging to that shard.

    Setup: 6 partitions, shard_count=2, shard 0 owns partitions {0, 2, 4}.
    Two replicas (r1, r2) both use kafka_partition_shard_num=0 with the same keeper_path.
    """
    admin = k.get_admin_client(kafka_cluster)
    topic_name = "affinity_multi_replica_topic"
    num_partitions = 6
    shard_count = 2
    shared_keeper_path = "/clickhouse/test/affinity_multi_replica"

    k.kafka_create_topic(admin, topic_name, num_partitions=num_partitions)
    with k.existing_kafka_topic(admin, topic_name):
        for p in range(num_partitions):
            msgs = [json.dumps({"partition_id": p, "value": i}) for i in range(6)]
            kafka_produce_to_partition(kafka_cluster, topic_name, p, msgs)

        # Replica 1 for shard 0
        instance.query(
            f"""
            CREATE TABLE test.kafka_mr_r1 (partition_id UInt64, value UInt64)
            ENGINE = Kafka('{instance.cluster.kafka_host}:19092', '{topic_name}', '{topic_name}_cg_mr', 'JSONEachRow', '\\n')
            SETTINGS kafka_keeper_path = '{shared_keeper_path}',
                     kafka_replica_name = 'r1',
                     kafka_partition_shard_num = '0',
                     kafka_shard_count = {shard_count}
            SETTINGS allow_experimental_kafka_offsets_storage_in_keeper=1;

            CREATE TABLE test.dst_mr_r1 (partition_id UInt64, value UInt64)
            ENGINE = MergeTree() ORDER BY (partition_id, value);

            CREATE MATERIALIZED VIEW test.mv_mr_r1 TO test.dst_mr_r1 AS
            SELECT * FROM test.kafka_mr_r1;
            """
        )

        # Replica 2 for shard 0 (same keeper_path, same shard_num, different replica_name)
        instance.query(
            f"""
            CREATE TABLE test.kafka_mr_r2 (partition_id UInt64, value UInt64)
            ENGINE = Kafka('{instance.cluster.kafka_host}:19092', '{topic_name}', '{topic_name}_cg_mr', 'JSONEachRow', '\\n')
            SETTINGS kafka_keeper_path = '{shared_keeper_path}',
                     kafka_replica_name = 'r2',
                     kafka_partition_shard_num = '0',
                     kafka_shard_count = {shard_count}
            SETTINGS allow_experimental_kafka_offsets_storage_in_keeper=1;

            CREATE TABLE test.dst_mr_r2 (partition_id UInt64, value UInt64)
            ENGINE = MergeTree() ORDER BY (partition_id, value);

            CREATE MATERIALIZED VIEW test.mv_mr_r2 TO test.dst_mr_r2 AS
            SELECT * FROM test.kafka_mr_r2;
            """
        )

        # Both replicas together consume shard-0 partitions {0,2,4}
        wait_for_count(instance, "test.dst_mr_r1", 1)
        wait_for_count(instance, "test.dst_mr_r2", 1)

        # Both replicas only consume shard-0 partitions
        for tbl in ["test.dst_mr_r1", "test.dst_mr_r2"]:
            leaked = instance.query(
                f"SELECT count() FROM {tbl} WHERE partition_id % {shard_count} != 0"
            ).strip()
            assert leaked == "0", f"{tbl} leaked messages from shard-1 partitions"

        # Detach replica 2 to simulate failure
        instance.query("DETACH TABLE test.kafka_mr_r2")

        # Produce more messages to shard-0 partitions
        for p in [0, 2, 4]:
            msgs = [json.dumps({"partition_id": p, "value": 100 + i}) for i in range(5)]
            kafka_produce_to_partition(kafka_cluster, topic_name, p, msgs)

        # r1 must reclaim all shard-0 partitions and consume all 15 new messages
        r1_before = int(instance.query("SELECT count() FROM test.dst_mr_r1").strip())
        wait_for_count(instance, "test.dst_mr_r1", r1_before + 15)

        # Verify r1 consumed from ALL shard-0 partitions after r2 went away
        r1_partitions = instance.query(
            "SELECT DISTINCT partition_id FROM test.dst_mr_r1 ORDER BY partition_id"
        ).strip()
        assert r1_partitions == "0\n2\n4", (
            f"After r2 detach, r1 should own all shard-0 partitions, got: {r1_partitions}"
        )

        # No shard-1 leakage
        leaked_r1 = instance.query(
            f"SELECT count() FROM test.dst_mr_r1 WHERE partition_id % {shard_count} != 0"
        ).strip()
        assert leaked_r1 == "0", f"r1 leaked {leaked_r1} messages from shard-1 partitions"


if __name__ == "__main__":
    cluster.start()
    input("Cluster created, press any key to destroy...")
    cluster.shutdown()
