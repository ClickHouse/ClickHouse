import json
import re
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


def create_kafka_table(table_name, topic_name, keeper_path,
                       replica_name, consumer_group=None, settings=None):
    """
    Return SQL that creates ONLY the Kafka table -- no destination, no materialized view.

    The StorageKafka2 constructor writes /replicas/<replica_name> at CREATE TABLE time
    (StorageKafka2.cpp:217), but `threadFunc` only streams once a materialized view is
    attached. Creating the tables first therefore registers every replica without any of
    them consuming yet, which is what `attach_materialized_view` below relies on.
    """
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
    )


def attach_materialized_view(table_name):
    """
    Return SQL that attaches the destination table and materialized view, which is what
    actually starts consumption for `table_name`.
    """
    return (
        f"CREATE TABLE test.dst_{table_name} (key UInt64, value UInt64)"
        f" ENGINE = MergeTree() ORDER BY key;"
        f"CREATE MATERIALIZED VIEW test.mv_{table_name} TO test.dst_{table_name}"
        f" AS SELECT * FROM test.{table_name};"
    )


def wait_for_replicas_registered(kafka_cluster, keeper_path, expected, timeout=60):
    """
    Block until /replicas holds `expected` children.

    This removes the convergence race the tests used to wait out. `active_replica_count`
    is the number of children of /replicas (getActiveReplicasInfo), so once every replica
    is registered each consumer computes the correct node_quota on its very first poll and
    no replica ever over-claims -- there is nothing to shed and nothing to settle.

    NOTE: this relies on active_replica_count counting *registered* replicas. If it is ever
    changed to consult the ephemeral is_active node, this setup has to change too.
    """
    base = f"{keeper_path}/replicas"
    start = time.time()
    seen = []
    while time.time() - start < timeout:
        try:
            seen = zk_ls(kafka_cluster, base)
        except Exception:
            seen = []
        if len(seen) >= expected:
            return seen
        time.sleep(1)
    pytest.fail(
        f"Only {len(seen)}/{expected} replicas registered under {base} within {timeout}s: {seen}"
    )


def zk_ls(kafka_cluster, path):
    with KeeperClient.from_cluster(kafka_cluster, keeper_node="zoo1") as zk:
        return zk.ls(path)


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


def wait_for_partitions_claimed(instance, tables, num_partitions, timeout=240):
    """
    Poll `system.kafka_consumers` until every replica in `tables` holds at least one
    partition and the replicas together hold all `num_partitions`. Returns
    {table: partitions_held}.

    Deliberately weak, and here is why. `assignments` merges permanent AND temporary locks
    (KeeperHandlingConsumer::getStat reads both maps), so it cannot show the permanent
    distribution on its own. While one replica is still acquiring its share, the partitions
    it has not taken yet are momentarily free and a sibling picks them up as temporary
    locks -- so a node can transiently report more than its node_quota ([0,1,2] against a
    node_quota of 2 was observed on arm_asan_ubsan). Any tighter bound on the counts is
    therefore asserting something the code does not guarantee.

    What is guaranteed, and what this checks: every replica gets work, and the topic is
    fully claimed. That is enough to catch the bug this PR fixes -- on master one replica
    takes the whole topic and the other reports zero. The exact per-consumer quotas are
    asserted separately, and exactly, from the trace log.
    """
    start = time.time()
    last = None
    while time.time() - start < timeout:
        held = {}
        for table in tables:
            raw = instance.query(
                "SELECT sum(length(assignments.partition_id)) "
                "FROM system.kafka_consumers "
                f"WHERE database = 'test' AND table = '{table}'"
            ).strip()
            held[table] = int(raw) if raw and raw != "\\N" else 0
        last = held
        if all(v >= 1 for v in held.values()) and sum(held.values()) == num_partitions:
            return held
        time.sleep(2)
    pytest.fail(
        f"Partitions were never fully claimed with every replica busy "
        f"(expected {num_partitions} across {len(tables)} replicas); last seen: {last}"
    )


def wait_for_shard_partitions(instance, table, timeout=240):
    """
    Return the set of partition ids currently held by `table`'s consumers, waiting until it
    holds at least one. The affinity check cares about *which* partitions a replica holds,
    not how many -- unlike the count, shard membership is not affected by convergence or by
    temporary locks, since the affinity filter runs before anything is locked.
    """
    start = time.time()
    while time.time() - start < timeout:
        raw = instance.query(
            "SELECT arrayFlatten(groupArray(assignments.partition_id)) "
            "FROM system.kafka_consumers "
            f"WHERE database = 'test' AND table = '{table}'"
        ).strip()
        if raw and raw != "[]":
            return set(int(x) for x in raw.strip("[]").split(",") if x.strip())
        time.sleep(2)
    pytest.fail(f"{table} never acquired any partition within {timeout}s")


@pytest.mark.parametrize(
    "num_partitions, num_replicas, num_consumers, expected_node_quota, case",
    [
        (12, 2, 3, 6, "even split: node_quota 6, per-consumer 2/2/2"),
        ( 4, 2, 3, 2, "P < R*N: the max(...,1) clamp must be per node; third consumer idles"),
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

        # Phase 1: create every Kafka table, so all replicas register in Keeper.
        # None of them consumes yet -- there is no materialized view attached.
        instance.query("\n".join(
            create_kafka_table(
                table_name=f"kafka_{topic_name}_{replica}",
                topic_name=topic_name,
                consumer_group=topic_name,
                keeper_path=keeper_path,
                replica_name=replica,
                settings={
                    "kafka_num_consumers": num_consumers,
                    "kafka_thread_per_consumer": 1,
                },
            )
            for replica in replicas
        ))
        wait_for_replicas_registered(kafka_cluster, keeper_path, num_replicas)

        # Phase 2: start consumption. Every consumer now sees active_replicas == R on its
        # first poll, so each takes exactly its share and nothing has to be rebalanced.
        instance.query("\n".join(
            attach_materialized_view(f"kafka_{topic_name}_{replica}") for replica in replicas
        ))

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

        # End-to-end: the quota was enforced, not just computed. Every replica gets work
        # and the topic is fully claimed -- on master one replica takes everything.
        held = wait_for_partitions_claimed(
            instance,
            [f"kafka_{topic_name}_{r}" for r in replicas],
            num_partitions,
        )
        for replica in replicas:
            table = f"kafka_{topic_name}_{replica}"
            assert held[table] >= 1, (
                f"[{case}] {replica} holds no partitions; held = {held}"
            )


def test_multi_consumer_with_partition_affinity(kafka_cluster):
    """
    Affinity shrinks P without shrinking N, which is exactly what drives a cluster
    into the P < R*N regime. 8 partitions, kafka_shard_count=2, two replicas per
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

        replicas = [(sh, ri, f"s{sh}_r{ri}") for sh in (1, 2) for ri in (1, 2)]

        # Phase 1: register every replica of both shards before any of them consumes.
        instance.query("\n".join(
            create_kafka_table(
                table_name=f"kafka_aff_{replica}",
                topic_name=topic_name,
                consumer_group=f"{topic_name}_cg_s{sh}",
                keeper_path=keeper_path,
                replica_name=replica,
                settings={
                    "kafka_num_consumers": num_consumers,
                    "kafka_thread_per_consumer": 1,
                    # kafka_partition_shard_num is a String setting, so it has to render
                    # quoted. Passing an int makes create_settings_string emit it bare and
                    # CREATE TABLE fails with
                    #   Code: 170. Bad get: has UInt64, requested String
                    "kafka_partition_shard_num": str(sh),
                    "kafka_shard_count": shard_count,
                },
            )
            for sh, _ri, replica in replicas
        ))
        # All four replicas share one keeper_path, so /replicas holds all of them; the
        # shard filter in getActiveReplicasInfo then narrows each consumer to its own shard.
        wait_for_replicas_registered(kafka_cluster, keeper_path, len(replicas))

        # Phase 2: start consumption with every replica already visible.
        instance.query("\n".join(
            attach_materialized_view(f"kafka_aff_{replica}") for _sh, _ri, replica in replicas
        ))

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

                # Affinity's own contract: a replica must only ever hold partitions of its
                # own shard. Exact per-consumer counts are deliberately NOT asserted here --
                # the parametrized cases above already prove enforcement, while here the
                # counts are sensitive to convergence timing: `assignments` merges permanent
                # and temporary locks (KeeperHandlingConsumer::getStat), and while a replica
                # that registered first sheds its surplus the partitions it releases are
                # briefly picked up as temporary locks by its shard peer. That transient
                # shows up as e.g. [0,0,1,2] against a node_quota of 2.
                effective = shard_num - 1
                held = wait_for_shard_partitions(instance, table)
                assert held, f"{replica} holds no partitions"
                assert all(pid % shard_count == effective for pid in held), (
                    f"{replica} (shard {shard_num}) holds partitions from another shard: "
                    f"{sorted(held)} -- the affinity filter must be applied before locking"
                )


if __name__ == "__main__":
    cluster.start()
    input("Cluster created, press any key to destroy...")
    cluster.shutdown()
