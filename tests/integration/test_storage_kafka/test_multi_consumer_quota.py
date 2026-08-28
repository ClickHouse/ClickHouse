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
        "kafka_topic_new": "mc_quota_topic",
        "kafka_group_name_new": "mc_quota_group",
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


def get_lock_owners(kafka_cluster, keeper_path, topic_name, num_partitions, timeout=60):
    """
    Poll Keeper until all partitions are locked, then return a dict mapping
    replica_name -> set of locked partition ids.
    """
    base = f"{keeper_path}/topic_partition_locks"
    expected_locks = {f"{topic_name}_{pid}.lock" for pid in range(num_partitions)}
    with KeeperClient.from_cluster(kafka_cluster, keeper_node="zoo1") as zk:
        start = time.time()
        while time.time() - start < timeout:
            children = set(zk.ls(base))
            if children >= expected_locks:
                break
            time.sleep(1)
        else:
            pytest.fail(
                f"Timed out waiting for all {num_partitions} locks in Keeper: "
                f"got {len(children)}, expected {len(expected_locks)}"
            )

        owners = {}
        for lock in expected_locks:
            owner = zk.get(f"{base}/{lock}")
            owners.setdefault(owner, set())
            pid = int(lock.replace(f"{topic_name}_", "").replace(".lock", ""))
            owners[owner].add(pid)
        return owners


def create_kafka_with_mv(instance, table_name, topic_name, consumer_group,
                         keeper_path, replica_name, settings=None):
    """Create a Kafka table with a MergeTree destination and MV to trigger consumption."""
    query = k.generate_new_create_table_query(
        table_name=table_name,
        columns_def="key UInt64, value UInt64",
        database="test",
        topic_list=topic_name,
        consumer_group=consumer_group,
        keeper_path=keeper_path,
        replica_name=replica_name,
        settings=settings,
    )
    instance.query(f"""
        DROP TABLE IF EXISTS test.mv_{table_name};
        DROP TABLE IF EXISTS test.dst_{table_name};
        DROP TABLE IF EXISTS test.{table_name};

        {query};
        CREATE TABLE test.dst_{table_name} (key UInt64, value UInt64) ENGINE = MergeTree() ORDER BY key;
        CREATE MATERIALIZED VIEW test.mv_{table_name} TO test.dst_{table_name} AS SELECT * FROM test.{table_name};
    """)


def test_multi_consumer_fair_distribution(kafka_cluster):
    """
    12 partitions, 2 replicas, kafka_num_consumers=3 on each.

    Node quota = 12 / 2 = 6. Each consumer's permanent quota:
      idx 0: 6/3 + (0 < 0 ? 1:0) = 2
      idx 1: 6/3 + (1 < 0 ? 1:0) = 2
      idx 2: 6/3 + (2 < 0 ? 1:0) = 2
    Per-node total: 6. Each replica holds exactly 6 partitions.

    Verified by inspecting lock ownership directly in Keeper.
    """
    admin = k.get_admin_client(kafka_cluster)
    topic_name = "mc_quota_fair_12p"
    num_partitions = 12
    num_consumers = 3
    keeper_path = "/clickhouse/test/mc_quota_fair"

    k.kafka_create_topic(admin, topic_name, num_partitions=num_partitions)
    with k.existing_kafka_topic(admin, topic_name):
        for p in range(num_partitions):
            msgs = [json.dumps({"key": p, "value": 1})]
            k.kafka_produce(kafka_cluster, topic_name, msgs, retries=5)

        for replica in ["r1", "r2"]:
            create_kafka_with_mv(
                instance,
                table_name=f"kafka_fair_{replica}",
                topic_name=topic_name,
                consumer_group=topic_name,
                keeper_path=keeper_path,
                replica_name=replica,
                settings={
                    "kafka_num_consumers": num_consumers,
                    "kafka_thread_per_consumer": 1,
                },
            )

        owners = get_lock_owners(kafka_cluster, keeper_path, topic_name, num_partitions)

        assert "r1" in owners, f"r1 has no locks; owners: {owners}"
        assert "r2" in owners, f"r2 has no locks; owners: {owners}"
        assert len(owners["r1"]) == 6, (
            f"r1 should hold 6 partitions, got {len(owners['r1'])}: {sorted(owners['r1'])}"
        )
        assert len(owners["r2"]) == 6, (
            f"r2 should hold 6 partitions, got {len(owners['r2'])}: {sorted(owners['r2'])}"
        )
        assert owners["r1"].isdisjoint(owners["r2"]), "Partitions must be exclusively assigned"


def test_multi_consumer_small_topic_p_less_than_rn(kafka_cluster):
    """
    P < R * N: 4 partitions, 2 replicas, kafka_num_consumers=3.

    Node quota = max(4/2, 1) = 2. Per-consumer:
      idx 0: 2/3 + (0 < 2%3 ? 1:0) = 0 + 1 = 1
      idx 1: 2/3 + (1 < 2%3 ? 1:0) = 0 + 1 = 1
      idx 2: 2/3 + (2 < 2%3 ? 1:0) = 0 + 0 = 0
    Per-node total permanent: 2. Each replica holds exactly 2.

    Without the node-share-first fix, max(4/(2*3), 1) = 1 per consumer,
    so per-node = 3, and two nodes would try to hold 6 > 4 total.
    """
    admin = k.get_admin_client(kafka_cluster)
    topic_name = "mc_quota_small_4p"
    num_partitions = 4
    num_consumers = 3
    keeper_path = "/clickhouse/test/mc_quota_small"

    k.kafka_create_topic(admin, topic_name, num_partitions=num_partitions)
    with k.existing_kafka_topic(admin, topic_name):
        for p in range(num_partitions):
            msgs = [json.dumps({"key": p, "value": 1})]
            k.kafka_produce(kafka_cluster, topic_name, msgs, retries=5)

        for replica in ["r1", "r2"]:
            create_kafka_with_mv(
                instance,
                table_name=f"kafka_small_{replica}",
                topic_name=topic_name,
                consumer_group=topic_name,
                keeper_path=keeper_path,
                replica_name=replica,
                settings={
                    "kafka_num_consumers": num_consumers,
                    "kafka_thread_per_consumer": 1,
                },
            )

        owners = get_lock_owners(kafka_cluster, keeper_path, topic_name, num_partitions)

        assert "r1" in owners, f"r1 has no locks; owners: {owners}"
        assert "r2" in owners, f"r2 has no locks; owners: {owners}"
        assert len(owners["r1"]) == 2, (
            f"r1 should hold 2 partitions (P<R*N case), got {len(owners['r1'])}: {sorted(owners['r1'])}"
        )
        assert len(owners["r2"]) == 2, (
            f"r2 should hold 2 partitions (P<R*N case), got {len(owners['r2'])}: {sorted(owners['r2'])}"
        )
        assert owners["r1"].isdisjoint(owners["r2"]), "Partitions must be exclusively assigned"


def test_multi_consumer_remainder_distribution(kafka_cluster):
    """
    Non-divisible remainder: 7 partitions, 1 replica, kafka_num_consumers=3.

    Node quota = max(7/1, 1) = 7. Per-consumer:
      idx 0: 7/3 + (0 < 7%3 ? 1:0) = 2 + 1 = 3
      idx 1: 7/3 + (1 < 7%3 ? 1:0) = 2 + 0 = 2  (7%3=1, so only idx 0 gets +1)
      idx 2: 7/3 + (2 < 7%3 ? 1:0) = 2 + 0 = 2
    Total: 7 = all partitions covered.

    With a single replica all 7 partitions must be locked (no competing node).
    """
    admin = k.get_admin_client(kafka_cluster)
    topic_name = "mc_quota_remainder_7p"
    num_partitions = 7
    num_consumers = 3
    keeper_path = "/clickhouse/test/mc_quota_remainder"

    k.kafka_create_topic(admin, topic_name, num_partitions=num_partitions)
    with k.existing_kafka_topic(admin, topic_name):
        for p in range(num_partitions):
            msgs = [json.dumps({"key": p, "value": 1})]
            k.kafka_produce(kafka_cluster, topic_name, msgs, retries=5)

        create_kafka_with_mv(
            instance,
            table_name="kafka_remainder",
            topic_name=topic_name,
            consumer_group=topic_name,
            keeper_path=keeper_path,
            replica_name="r1",
            settings={
                "kafka_num_consumers": num_consumers,
                "kafka_thread_per_consumer": 1,
            },
        )

        owners = get_lock_owners(kafka_cluster, keeper_path, topic_name, num_partitions)

        assert "r1" in owners, f"r1 has no locks; owners: {owners}"
        assert len(owners["r1"]) == num_partitions, (
            f"Single replica should hold all {num_partitions} partitions, "
            f"got {len(owners['r1'])}: {sorted(owners['r1'])}"
        )


def test_single_consumer_unchanged(kafka_cluster):
    """
    With kafka_num_consumers=1 (the default), the formula reduces to the
    original behaviour: can_lock = node_quota = P/R.
    6 partitions, 3 replicas -> 2 each.
    """
    admin = k.get_admin_client(kafka_cluster)
    topic_name = "mc_quota_single_6p"
    num_partitions = 6
    keeper_path = "/clickhouse/test/mc_quota_single"

    k.kafka_create_topic(admin, topic_name, num_partitions=num_partitions)
    with k.existing_kafka_topic(admin, topic_name):
        for p in range(num_partitions):
            msgs = [json.dumps({"key": p, "value": 1})]
            k.kafka_produce(kafka_cluster, topic_name, msgs, retries=5)

        for replica in ["r1", "r2", "r3"]:
            create_kafka_with_mv(
                instance,
                table_name=f"kafka_single_{replica}",
                topic_name=topic_name,
                consumer_group=topic_name,
                keeper_path=keeper_path,
                replica_name=replica,
            )

        owners = get_lock_owners(kafka_cluster, keeper_path, topic_name, num_partitions)

        total_locked = sum(len(pids) for pids in owners.values())
        assert total_locked == num_partitions, (
            f"All {num_partitions} partitions should be locked, got {total_locked}"
        )
        for replica in ["r1", "r2", "r3"]:
            assert replica in owners, f"{replica} has no locks; owners: {owners}"
            assert len(owners[replica]) == 2, (
                f"{replica} should hold 2 partitions, got {len(owners[replica])}"
            )


def test_multi_consumer_with_partition_affinity(kafka_cluster):
    """
    Partition affinity + multi-consumer: 8 partitions, shard_count=2,
    2 replicas per shard, kafka_num_consumers=4.

    Affinity assigns partitions by pid % shard_count:
      shard 1 (effective 0): partitions 0, 2, 4, 6  (4 partitions)
      shard 2 (effective 1): partitions 1, 3, 5, 7  (4 partitions)

    Each shard has 2 replicas. Per-shard: node_quota = max(4/2, 1) = 2.
    Per-consumer (N=4): quota(idx) = 2/4 + (idx < 2%4 ? 1:0)
      idx 0: 0 + 1 = 1
      idx 1: 0 + 1 = 1
      idx 2: 0 + 0 = 0
      idx 3: 0 + 0 = 0
    Per-node total permanent: 2. Each replica of each shard holds exactly 2.

    Verified by inspecting lock ownership directly in Keeper.
    """
    admin = k.get_admin_client(kafka_cluster)
    topic_name = "mc_quota_affinity_8p"
    num_partitions = 8
    num_consumers = 4
    shard_count = 2
    keeper_path = "/clickhouse/test/mc_quota_affinity"

    k.kafka_create_topic(admin, topic_name, num_partitions=num_partitions)
    with k.existing_kafka_topic(admin, topic_name):
        for p in range(num_partitions):
            msgs = [json.dumps({"key": p, "value": 1})]
            k.kafka_produce(kafka_cluster, topic_name, msgs, retries=5)

        for shard_num in [1, 2]:
            for replica_idx in [1, 2]:
                replica = f"s{shard_num}_r{replica_idx}"
                create_kafka_with_mv(
                    instance,
                    table_name=f"kafka_aff_{replica}",
                    topic_name=topic_name,
                    consumer_group=f"{topic_name}_cg_s{shard_num}",
                    keeper_path=keeper_path,
                    replica_name=replica,
                    settings={
                        "kafka_num_consumers": num_consumers,
                        "kafka_thread_per_consumer": 1,
                        "kafka_partition_shard_num": shard_num,
                        "kafka_shard_count": shard_count,
                    },
                )

        owners = get_lock_owners(kafka_cluster, keeper_path, topic_name, num_partitions)

        total_locked = sum(len(pids) for pids in owners.values())
        assert total_locked == num_partitions, (
            f"All {num_partitions} partitions should be locked, got {total_locked}"
        )

        for shard_num in [1, 2]:
            effective = shard_num - 1
            shard_partitions = {p for p in range(num_partitions) if p % shard_count == effective}
            shard_replicas = [f"s{shard_num}_r1", f"s{shard_num}_r2"]

            shard_locked = set()
            for replica in shard_replicas:
                if replica in owners:
                    shard_locked |= owners[replica]

            assert shard_locked == shard_partitions, (
                f"Shard {shard_num} should own partitions {sorted(shard_partitions)}, "
                f"got {sorted(shard_locked)}"
            )

            for replica in shard_replicas:
                assert replica in owners, f"{replica} has no locks; owners: {owners}"
                assert len(owners[replica]) == 2, (
                    f"{replica} should hold 2 partitions (4 partitions / 2 replicas), "
                    f"got {len(owners[replica])}: {sorted(owners[replica])}"
                )


if __name__ == "__main__":
    cluster.start()
    input("Cluster created, press any key to destroy...")
    cluster.shutdown()
