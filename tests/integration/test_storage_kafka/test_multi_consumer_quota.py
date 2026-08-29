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


def wait_for_lock_owners(kafka_cluster, keeper_path, topic_name, num_partitions,
                         converged=None, timeout=90):
    """
    Poll Keeper until all partition locks exist and the distribution satisfies
    the `converged` predicate (called with the owners dict).  Returns the
    owners dict: replica_name -> set of locked partition ids.

    Handles the TOCTOU race where a lock znode disappears between ls and get
    during rebalancing by retrying the whole snapshot.
    """
    base = f"{keeper_path}/topic_partition_locks"
    expected_locks = {f"{topic_name}_{pid}.lock" for pid in range(num_partitions)}
    with KeeperClient.from_cluster(kafka_cluster, keeper_node="zoo1") as zk:
        start = time.time()
        last_owners = None
        while time.time() - start < timeout:
            try:
                children = set(zk.ls(base))
            except Exception:
                time.sleep(1)
                continue

            if not children >= expected_locks:
                time.sleep(1)
                continue

            owners = {}
            snapshot_valid = True
            for lock in expected_locks:
                try:
                    owner = zk.get(f"{base}/{lock}")
                except Exception:
                    snapshot_valid = False
                    break
                owners.setdefault(owner, set())
                pid = int(lock.replace(f"{topic_name}_", "").replace(".lock", ""))
                owners[owner].add(pid)

            if not snapshot_valid:
                time.sleep(1)
                continue

            last_owners = owners
            if converged is None or converged(owners):
                return owners
            time.sleep(2)

        if last_owners is not None:
            return last_owners
        pytest.fail(
            f"Timed out waiting for stable lock distribution in Keeper "
            f"({num_partitions} partitions, timeout={timeout}s)"
        )


def create_kafka_with_mv(instance, table_name, topic_name, keeper_path,
                         replica_name, consumer_group=None, settings=None):
    """Return SQL that creates a Kafka table + MergeTree destination + MV."""
    query = k.generate_new_create_table_query(
        table_name=table_name,
        columns_def="key UInt64, value UInt64",
        database="test",
        topic_list=topic_name,
        consumer_group=consumer_group or topic_name,
        keeper_path=keeper_path,
        replica_name=replica_name,
        settings=settings,
    )
    return (
        f"DROP TABLE IF EXISTS test.mv_{table_name};"
        f"DROP TABLE IF EXISTS test.dst_{table_name};"
        f"DROP TABLE IF EXISTS test.{table_name};"
        f"{query};"
        f"CREATE TABLE test.dst_{table_name} (key UInt64, value UInt64)"
        f" ENGINE = MergeTree() ORDER BY key;"
        f"CREATE MATERIALIZED VIEW test.mv_{table_name} TO test.dst_{table_name}"
        f" AS SELECT * FROM test.{table_name};"
    )


@pytest.mark.parametrize(
    "num_partitions, num_replicas, num_consumers, min_per_replica, max_per_replica, case",
    [
        (12, 2, 3, 6, 6, "even split: node_quota 6, per-consumer 2/2/2"),
        ( 4, 2, 3, 2, 2, "P < R*N: the max(...,1) clamp must be per node, not per consumer"),
        ( 7, 2, 3, 3, 4, "P mod R != 0: remainder distributed; 1 partition floats on temp locks"),
        ( 6, 3, 1, 2, 2, "N=1: formula provably reduces to the pre-existing one"),
    ],
    ids=["even", "small_topic", "remainder", "single_consumer"],
)
def test_permanent_lock_quota(
    kafka_cluster, num_partitions, num_replicas, num_consumers,
    min_per_replica, max_per_replica, case,
):
    """
    node_quota = max(P / R, 1); each consumer takes
        node_quota / N + (idx < node_quota % N ? 1 : 0)
    so a node's consumers sum to exactly node_quota.  Verified by reading lock
    ownership out of Keeper, where each lock znode's data is the owning replica_name.
    """
    admin = k.get_admin_client(kafka_cluster)
    topic_name = f"quota_{num_partitions}p_{num_replicas}r_{num_consumers}c"
    keeper_path = f"/clickhouse/test/{topic_name}"
    replicas = [f"r{i}" for i in range(1, num_replicas + 1)]

    k.kafka_create_topic(admin, topic_name, num_partitions=num_partitions)
    with k.existing_kafka_topic(admin, topic_name):
        for p in range(num_partitions):
            k.kafka_produce(kafka_cluster, topic_name,
                            [json.dumps({"key": p, "value": 1})], retries=5)

        queries = []
        for replica in replicas:
            queries.append(create_kafka_with_mv(
                instance,
                table_name=f"kafka_{topic_name}_{replica}",
                topic_name=topic_name,
                consumer_group=topic_name,
                keeper_path=keeper_path,
                replica_name=replica,
                settings={
                    "kafka_num_consumers": num_consumers,
                    "kafka_thread_per_consumer": 1,
                },
            ))
        instance.query("\n".join(queries))

        def converged(owners):
            if sum(len(pids) for pids in owners.values()) != num_partitions:
                return False
            return all(min_per_replica <= len(owners.get(r, ())) <= max_per_replica
                       for r in replicas)

        owners = wait_for_lock_owners(
            kafka_cluster, keeper_path, topic_name, num_partitions, converged
        )

        for replica in replicas:
            held = len(owners.get(replica, ()))
            assert min_per_replica <= held <= max_per_replica, (
                f"[{case}] {replica} holds {held} partitions, expected "
                f"{min_per_replica}..{max_per_replica}; owners: {owners}"
            )


if __name__ == "__main__":
    cluster.start()
    input("Cluster created, press any key to destroy...")
    cluster.shutdown()
