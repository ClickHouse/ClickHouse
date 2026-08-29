import json
import re
import time

import pytest

from helpers.cluster import ClickHouseCluster
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


QUOTA_LOG_RE = re.compile(
    r"The consumer can have (\d+) permanent locks after the current round "
    r"\(node_quota=(\d+), active_replicas=(\d+), num_consumers=(\d+), idx=(\d+)\)"
)


def wait_for_quota_logs(instance, table_name, num_consumers, num_replicas,
                        timeout=90):
    """
    Wait until the server log shows the permanent-lock quota line for every
    consumer of the given table, with active_replicas == num_replicas (i.e.
    all replicas have registered).

    Returns a list of (can_lock, node_quota, active_replicas, num_consumers,
    idx) tuples, one per consumer.
    """
    start = time.time()
    hits = {}
    while time.time() - start < timeout:
        log = instance.grep_in_log(
            f"permanent locks after the current round"
        )
        for line in log.splitlines():
            if table_name not in line:
                continue
            m = QUOTA_LOG_RE.search(line)
            if not m:
                continue
            can_lock, nq, ar, nc, idx = (int(x) for x in m.groups())
            if ar >= num_replicas:
                hits[idx] = (can_lock, nq, ar, nc, idx)
        if len(hits) >= num_consumers:
            return list(hits.values())
        time.sleep(2)
    pytest.fail(
        f"Timed out waiting for permanent-lock quota log lines for {table_name} "
        f"(saw {len(hits)}/{num_consumers} consumers, timeout={timeout}s)"
    )


@pytest.mark.parametrize(
    "num_partitions, num_replicas, num_consumers, expected_node_quota, case",
    [
        (12, 2, 3, 6, "even split: node_quota 6, per-consumer 2/2/2"),
        ( 4, 2, 3, 2, "P < R*N: the max(...,1) clamp must be per node, not per consumer"),
        ( 7, 2, 3, 3, "P mod R != 0: node_quota 3, per-consumer 1/1/1"),
        ( 6, 3, 1, 2, "N=1: formula reduces to the pre-existing one"),
    ],
    ids=["even", "small_topic", "remainder", "single_consumer"],
)
def test_permanent_lock_quota(
    kafka_cluster, num_partitions, num_replicas, num_consumers,
    expected_node_quota, case,
):
    """
    Verify that the per-consumer permanent-lock quota is computed correctly:

        node_quota   = max(P / R, 1)
        per_consumer = node_quota / N + (idx < node_quota % N ? 1 : 0)

    so a node's N consumers sum to exactly node_quota.

    We verify the formula by parsing the server trace log that prints the
    computed quota for each consumer.  This avoids counting Keeper lock znodes,
    which include both permanent and temporary locks and are subject to timing
    races that make exact bounds unreliable.
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

        for replica in replicas:
            table = f"kafka_{topic_name}_{replica}"
            quotas = wait_for_quota_logs(
                instance, table, num_consumers, num_replicas
            )

            total = sum(q[0] for q in quotas)
            assert total == expected_node_quota, (
                f"[{case}] {replica}: sum of per-consumer quotas = {total}, "
                f"expected node_quota = {expected_node_quota}; quotas = {quotas}"
            )

            for can_lock, nq, ar, nc, idx in quotas:
                expected = expected_node_quota // num_consumers + (
                    1 if idx < expected_node_quota % num_consumers else 0
                )
                assert can_lock == expected, (
                    f"[{case}] {replica} consumer idx={idx}: can_lock={can_lock}, "
                    f"expected {expected} (node_quota={nq}, N={nc})"
                )

                assert nq == expected_node_quota, (
                    f"[{case}] {replica} consumer idx={idx}: "
                    f"node_quota={nq}, expected {expected_node_quota}"
                )
                assert ar == num_replicas, (
                    f"[{case}] {replica} consumer idx={idx}: "
                    f"active_replicas={ar}, expected {num_replicas}"
                )


if __name__ == "__main__":
    cluster.start()
    input("Cluster created, press any key to destroy...")
    cluster.shutdown()
