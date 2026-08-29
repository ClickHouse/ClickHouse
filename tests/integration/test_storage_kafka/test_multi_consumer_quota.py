import ast
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
        log = instance.grep_in_log("permanent locks after the current round")
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


def wait_for_consumer_assignments(instance, table, acceptable, timeout=240):
    """
    Poll `system.kafka_consumers` until this table's consumers report one of the
    `acceptable` multisets of assignment sizes (sorted ascending).

    This verifies the quota was actually *enforced*, not merely computed: the log
    assertions above read what the code decided, this reads what it holds.

    The timeout is deliberately generous. Unlike the log assertions, this waits for
    the distribution to CONVERGE. A replica that registers first sees
    active_replicas=1, takes the whole node_quota, and only sheds the surplus on a
    later refresh round (LOCKS_REFRESH_POLLS polls apart). Under asan/ubsan that has
    been observed to take well over 90s -- see the 9/3-instead-of-6/6 failure in the
    flaky check on ef41915a.

    NOTE: `assignments` merges permanent AND temporary locks (see
    KeeperHandlingConsumer::getStat), so any case with `P mod R != 0` must also
    accept the extra partition a consumer may hold transiently.
    """
    start = time.time()
    last = None
    while time.time() - start < timeout:
        raw = instance.query(
            "SELECT arraySort(groupArray(length(assignments.partition_id))) "
            "FROM system.kafka_consumers "
            f"WHERE database = 'test' AND table = '{table}'"
        ).strip()
        last = raw
        if raw:
            counts = ast.literal_eval(raw)
            if counts in acceptable:
                return counts
        time.sleep(2)
    pytest.fail(
        f"Per-consumer assignments for {table} never matched any of {acceptable}; "
        f"last seen: {last}"
    )


@pytest.mark.parametrize(
    "num_partitions, num_replicas, num_consumers, expected_node_quota, "
    "acceptable_consumer_counts, case",
    [
        (12, 2, 3, 6, [[2, 2, 2]],
         "even split: node_quota 6, per-consumer 2/2/2"),
        ( 4, 2, 3, 2, [[0, 1, 1]],
         "P < R*N: the max(...,1) clamp must be per node; third consumer idles"),
        ( 7, 2, 3, 3, [[1, 1, 1], [1, 1, 2]],
         "P mod R != 0: node_quota 3, per-consumer 1/1/1 (+1 if holding the float)"),
        ( 6, 3, 1, 2, [[2]],
         "N=1: formula reduces to the pre-existing one"),
    ],
    ids=["even", "small_topic", "remainder", "single_consumer"],
)
def test_permanent_lock_quota(
    kafka_cluster, num_partitions, num_replicas, num_consumers,
    expected_node_quota, acceptable_consumer_counts, case,
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

            # End-to-end: the quota was not just computed, it was enforced.
            counts = wait_for_consumer_assignments(
                instance, table, acceptable_consumer_counts
            )
            assert counts in acceptable_consumer_counts, (
                f"[{case}] {replica} per-consumer assignments {counts}, "
                f"expected one of {acceptable_consumer_counts}"
            )


def test_multi_consumer_with_partition_affinity(kafka_cluster):
    """
    Affinity shrinks P without shrinking N, which is exactly what drives a cluster
    into the P < R*N regime.  8 partitions, kafka_shard_count=2, two replicas per
    shard, kafka_num_consumers=4.

      Each shard sees P_shard = 4 and R_shard = 2  ->  node_quota = max(4/2, 1) = 2
      split over 4 consumers -> 1, 1, 0, 0         ->  2 locks per replica, 8 total.

    Fails on master, where max(4/2, 1) = 2 per consumer x 4 consumers lets a single
    replica take an entire shard.
    """
    admin = k.get_admin_client(kafka_cluster)
    topic_name = "quota_affinity_8p"
    keeper_path = f"/clickhouse/test/{topic_name}"
    num_partitions = 8
    shard_count = 2
    num_consumers = 4
    expected_node_quota = 2

    k.kafka_create_topic(admin, topic_name, num_partitions=num_partitions)
    with k.existing_kafka_topic(admin, topic_name):
        for p in range(num_partitions):
            k.kafka_produce(kafka_cluster, topic_name,
                            [json.dumps({"key": p, "value": 1})], retries=5)

        queries = []
        for shard_num in (1, 2):
            for replica_idx in (1, 2):
                replica = f"s{shard_num}_r{replica_idx}"
                queries.append(create_kafka_with_mv(
                    instance,
                    table_name=f"kafka_aff_{replica}",
                    topic_name=topic_name,
                    consumer_group=f"{topic_name}_cg_s{shard_num}",
                    keeper_path=keeper_path,
                    replica_name=replica,
                    settings={
                        "kafka_num_consumers": num_consumers,
                        "kafka_thread_per_consumer": 1,
                        # kafka_partition_shard_num is a String setting, so it has to
                        # render quoted.  Passing an int makes create_settings_string
                        # emit it bare and CREATE TABLE fails with
                        #   Code: 170. Bad get: has UInt64, requested String
                        "kafka_partition_shard_num": str(shard_num),
                        "kafka_shard_count": shard_count,
                    },
                ))
        instance.query("\n".join(queries))

        for shard_num in (1, 2):
            for replica_idx in (1, 2):
                replica = f"s{shard_num}_r{replica_idx}"
                table = f"kafka_aff_{replica}"

                quotas = wait_for_quota_logs(instance, table, num_consumers, 2)

                total = sum(q[0] for q in quotas)
                assert total == expected_node_quota, (
                    f"{replica}: sum of per-consumer quotas = {total}, expected "
                    f"{expected_node_quota} (P_shard 4 / R_shard 2); quotas = {quotas}"
                )

                for _can_lock, nq, ar, _nc, idx in quotas:
                    assert nq == expected_node_quota, (
                        f"{replica} idx={idx}: node_quota={nq}, expected "
                        f"{expected_node_quota} — affinity filter must shrink "
                        f"topic_partitions_count before the quota is computed"
                    )
                    assert ar == 2, (
                        f"{replica} idx={idx}: active_replicas={ar}, expected 2 "
                        f"(only replicas of the same shard should be counted)"
                    )

                counts = wait_for_consumer_assignments(
                    instance, table, [[0, 0, 1, 1]]
                )
                assert counts == [0, 0, 1, 1], (
                    f"{replica} per-consumer assignments {counts}, expected [0, 0, 1, 1]"
                )


if __name__ == "__main__":
    cluster.start()
    input("Cluster created, press any key to destroy...")
    cluster.shutdown()
