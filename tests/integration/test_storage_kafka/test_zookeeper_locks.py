import json
import time
import pytest

from helpers.cluster import ClickHouseCluster
import helpers.kafka.common as k

from helpers.keeper_utils import KeeperClient

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "instance",
    main_configs=["configs/kafka_and_keeper.xml"],
    user_configs=["configs/users.xml"],
    with_kafka=True,
    with_zookeeper=True,
    macros={
        "kafka_broker": "kafka1",
        "kafka_topic_new": "zk_locks_topic",
        "kafka_group_name_new": "zk_locks_group",
        "kafka_client_id": "instance",
        "kafka_format_json_each_row": "JSONEachRow",
    }
)


# Fixtures
@pytest.fixture(scope="module")
def kafka_cluster():
    try:
        cluster.start()
        kafka_id = instance.cluster.kafka_docker_id
        print(("kafka_id is {}".format(kafka_id)))
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def kafka_setup_teardown():
    k.clean_test_database_and_topics(instance, cluster)
    yield  # run test


# Tests


def test_zookeeper_partition_locks(kafka_cluster):
    admin = k.get_admin_client(kafka_cluster)
    num_partitions = 3
    topic_name = "zk_locks_topic"
    keeper_path = "/clickhouse/test/zk_locks"

    k.kafka_create_topic(admin, "zk_locks_topic", num_partitions=num_partitions)
    with k.existing_kafka_topic(admin, topic_name):
        create_kafka = k.generate_new_create_table_query(
            table_name="kafka",
            columns_def="key UInt64, value UInt64",
            database="test",
            topic_list=topic_name,
            consumer_group=topic_name,
            keeper_path=keeper_path,
            replica_name="r1"
        )
        instance.query(
            f"""
            DROP TABLE IF EXISTS test.kafka;
            DROP TABLE IF EXISTS test.view;
            DROP TABLE IF EXISTS test.consumer;

            {create_kafka};
            CREATE TABLE test.view (key UInt64, value UInt64) ENGINE = MergeTree() ORDER BY key;
            CREATE MATERIALIZED VIEW test.consumer TO test.view AS SELECT * FROM test.kafka;
            """
        )

        messages = []
        for i in range(num_partitions):
            messages.append(json.dumps({"key": i, "value": i}))
        k.kafka_produce(kafka_cluster, topic_name, messages, retries=5)

        base = f"{keeper_path}/topic_partition_locks"
        expected_locks = {f"zk_locks_topic_{pid}.lock" for pid in range(num_partitions)}
        with KeeperClient.from_cluster(kafka_cluster, keeper_node="zoo1") as zk:
            start = time.time()
            timeout, interval = 30.0, 1.0
            while time.time() - start < timeout:
                children = set(zk.ls(base))
                if children == expected_locks:
                    break
                time.sleep(interval)
            else:
                pytest.fail(f"Timed out waiting for locks in ZK: got {children!r}, expected {expected_locks!r}")

            for lock in expected_locks:
                owner = zk.get(f"{base}/{lock}")
                assert owner == "r1", f"Expected 'r1' in {lock}, got {owner}"


def test_three_replicas_ten_partitions_rebalance(kafka_cluster):
    admin = k.get_admin_client(kafka_cluster)
    topic_name= "zk_dist_topic_10p"
    num_partitions = 10
    replica_names = ["r1", "r2", "r3"]
    keeper_path = "/clickhouse/test/zk_dist3"

    k.kafka_create_topic(admin, topic_name, num_partitions=num_partitions)
    with k.existing_kafka_topic(admin, topic_name):
        create_kafka_queries = [
            k.generate_new_create_table_query(
                table_name=f"kafka_{replica}",
                columns_def="key UInt64, value UInt64",
                database="test",
                topic_list=topic_name,
                consumer_group=topic_name,
                keeper_path=keeper_path,
                replica_name=replica
            ) + ";"
            for replica in replica_names
        ]
        view_queries = [
            f"CREATE TABLE test.view_{replica} (key UInt64, value UInt64) ENGINE = MergeTree() ORDER BY key;"
            for replica in replica_names
        ]
        mv_queries = [
            f"CREATE MATERIALIZED VIEW test.cons_{replica} TO test.view_{replica} AS SELECT * FROM test.kafka_{replica};"
            for replica in replica_names
        ]
        drop_queries = [
            f"DROP TABLE IF EXISTS test.kafka_{replica};"
            f"DROP TABLE IF EXISTS test.view_{replica};"
            f"DROP TABLE IF EXISTS test.cons_{replica};"
            for replica in replica_names
        ]
        instance.query(
            "\n".join(drop_queries) + "\n" +
            "\n".join(create_kafka_queries) + "\n" +
            "\n".join(view_queries) + "\n" +
            "\n".join(mv_queries)
        )

        messages = []
        for i in range(num_partitions):
            messages.append(json.dumps({"key": i, "value": i}))
        k.kafka_produce(kafka_cluster, topic_name, messages, retries=5)

        base = f"{keeper_path}/topic_partition_locks"
        expected_locks = {f"{topic_name}_{pid}.lock" for pid in range(num_partitions)}
        with KeeperClient.from_cluster(kafka_cluster, keeper_node="zoo1") as zk:
            start = time.time()
            timeout, interval = 30.0, 1.0
            while time.time() - start < timeout:
                children = set(zk.ls(base))
                if children == expected_locks:
                    break
                time.sleep(interval)
            else:
                pytest.fail(f"Timed out waiting for locks in ZK: got {children!r}, expected {expected_locks!r}")

            counts = {replica: 0 for replica in replica_names}
            for lock in expected_locks:
                owner = zk.get(f"{base}/{lock}")
                if owner not in counts:
                    pytest.fail(f"Unknown owner {owner!r} for lock {lock}")
                counts[owner] += 1

            base_count = num_partitions // len(replica_names)
            values = sorted(counts.values())
            assert sum(values) == num_partitions
            assert all(v in (base_count-1, base_count, base_count+1) for v in values), f"Values: {values}"
            assert values[-1] - values[0] <= 2


def wait_for_locks(kafka_cluster, base, expected_locks, expected_owner, timeout=60.0, interval=1.0):
    """Wait until the lock set stabilizes to `expected_locks`, all owned by `expected_owner`."""
    start = time.time()
    owners = {}
    while time.time() - start < timeout:
        with KeeperClient.from_cluster(kafka_cluster, keeper_node="zoo1") as zk:
            children = [c for c in zk.ls(base) if c]
            owners = {lock: zk.get(f"{base}/{lock}") for lock in children}
        if set(owners) == expected_locks and all(v == expected_owner for v in owners.values()):
            return owners
        time.sleep(interval)
    pytest.fail(f"Timed out waiting for locks {expected_locks} owned by {expected_owner!r}, got {owners!r}")


def test_inactive_replica_not_counted(kafka_cluster):
    admin = k.get_admin_client(kafka_cluster)
    topic_name = "zk_inactive_replica_topic"
    num_partitions = 4
    keeper_path = "/clickhouse/test/zk_inactive_replica"

    k.kafka_create_topic(admin, topic_name, num_partitions=num_partitions)
    with k.existing_kafka_topic(admin, topic_name):
        create_kafka = k.generate_new_create_table_query(
            table_name="kafka",
            columns_def="key UInt64, value UInt64",
            database="test",
            topic_list=topic_name,
            consumer_group=topic_name,
            keeper_path=keeper_path,
            replica_name="r1"
        )
        # Create the Kafka table first: it registers `r1` in Keeper, but nothing is consumed and no
        # partition lock is taken until the materialized view is attached.
        instance.query(
            f"""
            DROP TABLE IF EXISTS test.kafka;
            DROP TABLE IF EXISTS test.view;
            DROP TABLE IF EXISTS test.consumer;

            {create_kafka};
            CREATE TABLE test.view (key UInt64, value UInt64) ENGINE = MergeTree() ORDER BY key;
            """
        )

        # Simulate a replica that died without cleaning up: a persistent replica znode without the
        # `is_active` ephemeral node. It is created before the first lock assignment, so the very
        # first `getActiveReplicasInfo` has to decide whether to count it.
        with KeeperClient.from_cluster(kafka_cluster, keeper_node="zoo1") as zk:
            zk.create(f"{keeper_path}/replicas/ghost", "0")
            assert set(zk.ls(f"{keeper_path}/replicas")) >= {"r1", "ghost"}

        # Now start consuming: the first lock assignment happens with the ghost replica present.
        instance.query(
            "CREATE MATERIALIZED VIEW test.consumer TO test.view AS SELECT * FROM test.kafka"
        )

        messages = [json.dumps({"key": i, "value": i}) for i in range(2 * num_partitions)]
        k.kafka_produce(kafka_cluster, topic_name, messages, retries=5)

        base = f"{keeper_path}/topic_partition_locks"
        expected_locks = {f"{topic_name}_{pid}.lock" for pid in range(num_partitions)}

        # The ghost replica must not be counted as active. If it were, the node quota would be
        # `num_partitions / 2`, so `r1` would hold only half of the partitions as permanent locks
        # (plus at most one temporary lock, because `has_replica_without_locks` keeps the temporary
        # quota at zero every other round) and would never own the whole lock set.
        wait_for_locks(kafka_cluster, base, expected_locks, "r1")

        instance.query_with_retry(
            "SELECT count() FROM test.view",
            check_callback=lambda res: int(res.strip()) >= len(messages),
            retry_count=60,
            sleep_time=1,
        )

        # The lock set must stay complete across a refresh round as well.
        k.kafka_produce(kafka_cluster, topic_name, messages, retries=5)
        wait_for_locks(kafka_cluster, base, expected_locks, "r1")

        instance.query_with_retry(
            "SELECT count() FROM test.view",
            check_callback=lambda res: int(res.strip()) >= 2 * len(messages),
            retry_count=60,
            sleep_time=1,
        )


def test_inactive_replica_not_counted_with_shard_affinity(kafka_cluster):
    """Same as `test_inactive_replica_not_counted`, but for the partition affinity path.

    With `kafka_shard_count` set, `getActiveReplicasInfo` first filters the replicas by the shard
    num stored as the replica znode data, and only then checks liveness. A same-shard replica that
    died without cleaning up its persistent znode must not shrink the quota of the live replica.
    """
    admin = k.get_admin_client(kafka_cluster)
    topic_name = "zk_inactive_replica_affinity_topic"
    num_partitions = 8
    shard_count = 2
    shard_num = 1
    keeper_path = "/clickhouse/test/zk_inactive_replica_affinity"

    k.kafka_create_topic(admin, topic_name, num_partitions=num_partitions)
    with k.existing_kafka_topic(admin, topic_name):
        create_kafka = k.generate_new_create_table_query(
            table_name="kafka",
            columns_def="key UInt64, value UInt64",
            database="test",
            topic_list=topic_name,
            consumer_group=topic_name,
            keeper_path=keeper_path,
            replica_name="r1",
            settings={
                # `kafka_partition_shard_num` is a String setting, so it has to render quoted;
                # `create_settings_string` quotes Python strings, but emits ints bare.
                "kafka_partition_shard_num": str(shard_num),
                "kafka_shard_count": shard_count,
            },
        )
        # Create the Kafka table first: it registers `r1` in Keeper, but nothing is consumed and no
        # partition lock is taken until the materialized view is attached.
        instance.query(
            f"""
            DROP TABLE IF EXISTS test.kafka;
            DROP TABLE IF EXISTS test.view;
            DROP TABLE IF EXISTS test.consumer;

            {create_kafka};
            CREATE TABLE test.view (key UInt64, value UInt64) ENGINE = MergeTree() ORDER BY key;
            """
        )

        with KeeperClient.from_cluster(kafka_cluster, keeper_node="zoo1") as zk:
            # A replica of our own shard that died without cleaning up: the persistent replica znode
            # carries our shard num, but there is no `is_active` node under it.
            zk.create(f"{keeper_path}/replicas/ghost_same_shard", str(shard_num))
            # A live replica of the other shard: it has `is_active`, but it must be filtered out by
            # the shard num, so it does not shrink our quota either.
            zk.create(f"{keeper_path}/replicas/live_other_shard", str(shard_num + 1))
            # The content of `is_active` is irrelevant, only its existence matters; the keeper
            # client cannot create a node with an empty value, so write a placeholder.
            zk.create(f"{keeper_path}/replicas/live_other_shard/is_active", "1")
            assert set(zk.ls(f"{keeper_path}/replicas")) >= {
                "r1", "ghost_same_shard", "live_other_shard"
            }

        # Now start consuming: the first lock assignment happens with both extra replicas present.
        instance.query(
            "CREATE MATERIALIZED VIEW test.consumer TO test.view AS SELECT * FROM test.kafka"
        )

        messages = [json.dumps({"key": i, "value": i}) for i in range(2 * num_partitions)]
        k.kafka_produce(kafka_cluster, topic_name, messages, retries=5)

        base = f"{keeper_path}/topic_partition_locks"
        # Affinity: our shard owns the partitions with `partition_id % shard_count == shard_num - 1`.
        own_partitions = [
            pid for pid in range(num_partitions) if pid % shard_count == shard_num - 1
        ]
        expected_locks = {f"{topic_name}_{pid}.lock" for pid in own_partitions}

        # `r1` is the only active replica of its shard, so it must own all of its shard's
        # partitions. If the ghost replica were counted, the node quota would be halved and `r1`
        # would never own the whole lock set of its shard.
        wait_for_locks(kafka_cluster, base, expected_locks, "r1")

        first_count = int(instance.query_with_retry(
            "SELECT count() FROM test.view",
            check_callback=lambda res: int(res.strip()) > 0,
            retry_count=60,
            sleep_time=1,
        ).strip())

        # The lock set must stay complete across a refresh round as well. Wait until the second
        # batch is actually consumed before re-checking the locks, otherwise `wait_for_locks`
        # could just re-read the unchanged Keeper state without any new poll cycle.
        k.kafka_produce(kafka_cluster, topic_name, messages, retries=5)
        instance.query_with_retry(
            "SELECT count() FROM test.view",
            check_callback=lambda res: int(res.strip()) > first_count,
            retry_count=60,
            sleep_time=1,
        )
        wait_for_locks(kafka_cluster, base, expected_locks, "r1")
