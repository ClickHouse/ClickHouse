import json
import time
import pytest

from helpers.cluster import ClickHouseCluster
import helpers.kafka.common as k

from helpers.keeper_utils import KeeperClient
from helpers.network import PartitionManager

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

        instance.query_with_retry(
            "SELECT count() FROM test.view",
            check_callback=lambda res: int(res.strip()) > 0,
            retry_count=60,
            sleep_time=1,
        )

        # The lock set must stay complete across a refresh round as well.
        k.kafka_produce(kafka_cluster, topic_name, messages, retries=5)
        wait_for_locks(kafka_cluster, base, expected_locks, "r1")


def test_own_registration_loss_triggers_reactivation(kafka_cluster):
    """The replica must recover when its own `is_active` node disappears from Keeper.

    `is_active` is ephemeral, but it can also be removed while the session is alive, e.g. from
    outside the server. Nothing re-creates it on its own, and since the partition locks are
    distributed by counting only active replicas, the replica would be left out of every peer's
    quota while still holding on to its own locks. The consumer has to notice that it is not among
    the active replicas anymore and hand control over to the normal deactivate/reactivate path.
    """
    admin = k.get_admin_client(kafka_cluster)
    topic_name = "zk_own_registration_loss_topic"
    num_partitions = 2
    keeper_path = "/clickhouse/test/zk_own_registration_loss"

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

        messages = [json.dumps({"key": i, "value": i}) for i in range(num_partitions)]
        k.kafka_produce(kafka_cluster, topic_name, messages, retries=5)

        base = f"{keeper_path}/topic_partition_locks"
        expected_locks = {f"{topic_name}_{pid}.lock" for pid in range(num_partitions)}
        wait_for_locks(kafka_cluster, base, expected_locks, "r1")

        instance.query_with_retry(
            "SELECT count() FROM test.view",
            check_callback=lambda res: int(res.strip()) >= len(messages),
            retry_count=60,
            sleep_time=1,
        )

        # Drop our own registration behind the server's back.
        is_active_path = f"{keeper_path}/replicas/r1/is_active"
        with KeeperClient.from_cluster(kafka_cluster, keeper_node="zoo1") as zk:
            zk.rm(is_active_path)
            assert "is_active" not in set(zk.ls(f"{keeper_path}/replicas/r1"))

        # The table must re-register itself instead of staying wedged without an `is_active` node.
        deadline = time.time() + 180.0
        while time.time() < deadline:
            with KeeperClient.from_cluster(kafka_cluster, keeper_node="zoo1") as zk:
                if "is_active" in set(zk.ls(f"{keeper_path}/replicas/r1")):
                    break
            time.sleep(1.0)
        else:
            pytest.fail(f"Timed out waiting for {is_active_path} to be re-created")

        # And it must keep consuming afterwards, with the full lock set back in place.
        k.kafka_produce(kafka_cluster, topic_name, messages, retries=5)
        wait_for_locks(kafka_cluster, base, expected_locks, "r1", timeout=120.0)

        instance.query_with_retry(
            "SELECT count() FROM test.view",
            check_callback=lambda res: int(res.strip()) >= 2 * len(messages),
            retry_count=120,
            sleep_time=1,
        )


def wait_for_replica_registration(kafka_cluster, keeper_path, replica_name, expected_data, timeout=180.0):
    """Wait until `replicas/<replica_name>` is back with the expected data and an `is_active` child."""
    replica_path = f"{keeper_path}/replicas/{replica_name}"
    deadline = time.time() + timeout
    last_seen = None
    while time.time() < deadline:
        with KeeperClient.from_cluster(kafka_cluster, keeper_node="zoo1") as zk:
            if replica_name in set(zk.ls(f"{keeper_path}/replicas")):
                last_seen = (zk.get(replica_path) or "").strip()
                if last_seen == expected_data and "is_active" in set(zk.ls(replica_path)):
                    return
        time.sleep(1.0)
    pytest.fail(
        f"Timed out waiting for {replica_path} to be restored with data '{expected_data}' "
        f"and an `is_active` node (last seen data: {last_seen})"
    )


def test_replica_znode_loss_triggers_reregistration(kafka_cluster):
    """The replica must recover when its whole persistent `replicas/<name>` znode disappears.

    Removing the persistent znode also takes the ephemeral `is_active` node with it, so the replica
    stops being counted as active by its peers. Re-creating only `is_active` is not enough here: the
    parent is gone, so every attempt would fail with `ZNONODE`. The reactivation path has to repair
    the whole registration.
    """
    admin = k.get_admin_client(kafka_cluster)
    topic_name = "zk_replica_znode_loss_topic"
    num_partitions = 2
    keeper_path = "/clickhouse/test/zk_replica_znode_loss"

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

        messages = [json.dumps({"key": i, "value": i}) for i in range(num_partitions)]
        k.kafka_produce(kafka_cluster, topic_name, messages, retries=5)

        base = f"{keeper_path}/topic_partition_locks"
        expected_locks = {f"{topic_name}_{pid}.lock" for pid in range(num_partitions)}
        wait_for_locks(kafka_cluster, base, expected_locks, "r1")

        instance.query_with_retry(
            "SELECT count() FROM test.view",
            check_callback=lambda res: int(res.strip()) >= len(messages),
            retry_count=60,
            sleep_time=1,
        )

        # Remove the whole registration behind the server's back, while its session stays alive.
        with KeeperClient.from_cluster(kafka_cluster, keeper_node="zoo1") as zk:
            zk.rmr(f"{keeper_path}/replicas/r1")
            assert "r1" not in set(zk.ls(f"{keeper_path}/replicas"))

        wait_for_replica_registration(kafka_cluster, keeper_path, "r1", "")

        # And it must keep consuming afterwards, with the full lock set back in place.
        k.kafka_produce(kafka_cluster, topic_name, messages, retries=5)
        wait_for_locks(kafka_cluster, base, expected_locks, "r1", timeout=120.0)

        instance.query_with_retry(
            "SELECT count() FROM test.view",
            check_callback=lambda res: int(res.strip()) >= 2 * len(messages),
            retry_count=120,
            sleep_time=1,
        )


def test_corrupted_shard_marker_triggers_reregistration(kafka_cluster):
    """The replica must recover when the shard num stored in its own znode drifts.

    In affinity mode the peers keep only the replicas whose znode data holds their own shard num, so
    a corrupted marker drops this replica out of the active set for everyone, itself included. The
    ephemeral `is_active` node is still there, so a check that only looks at it would take the
    `No need to activate` fast path and the table would stall forever.
    """
    admin = k.get_admin_client(kafka_cluster)
    topic_name = "zk_corrupted_shard_marker_topic"
    num_partitions = 4
    shard_count = 2
    shard_num = 1
    keeper_path = "/clickhouse/test/zk_corrupted_shard_marker"

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
                # `kafka_partition_shard_num` is a String setting, so it has to render quoted.
                "kafka_partition_shard_num": str(shard_num),
                "kafka_shard_count": shard_count,
            },
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

        messages = [json.dumps({"key": i, "value": i}) for i in range(num_partitions)]
        k.kafka_produce(kafka_cluster, topic_name, messages, retries=5)

        base = f"{keeper_path}/topic_partition_locks"
        own_partitions = [
            pid for pid in range(num_partitions) if pid % shard_count == shard_num - 1
        ]
        expected_locks = {f"{topic_name}_{pid}.lock" for pid in own_partitions}
        wait_for_locks(kafka_cluster, base, expected_locks, "r1")

        instance.query_with_retry(
            "SELECT count() FROM test.view",
            check_callback=lambda res: int(res.strip()) > 0,
            retry_count=60,
            sleep_time=1,
        )

        # Point the shard marker at the other shard behind the server's back. The `is_active` node
        # stays in place, so only a full validation of the registration can notice this.
        with KeeperClient.from_cluster(kafka_cluster, keeper_node="zoo1") as zk:
            zk.set(f"{keeper_path}/replicas/r1", str(shard_num + 1))
            assert (zk.get(f"{keeper_path}/replicas/r1") or "").strip() == str(shard_num + 1)

        wait_for_replica_registration(kafka_cluster, keeper_path, "r1", str(shard_num))

        # And it must keep consuming its own shard's partitions afterwards.
        k.kafka_produce(kafka_cluster, topic_name, messages, retries=5)
        wait_for_locks(kafka_cluster, base, expected_locks, "r1", timeout=120.0)

        instance.query_with_retry(
            "SELECT count() FROM test.view",
            check_callback=lambda res: int(res.strip()) >= 2 * len(own_partitions),
            retry_count=120,
            sleep_time=1,
        )


def test_registration_loss_releases_partition_locks(kafka_cluster):
    """The partition locks must be given up as soon as this replica stops being registered.

    The peers distribute the partitions among the replicas they can see, so once this replica is out of
    that set its old locks are not backed by anything: the partitions they cover stay wedged for everyone
    else. The lock holders are ephemeral, but the Keeper session is untouched here, so nothing expires
    them - the reactivation path itself has to drop them.

    To observe that, the registration is removed in a way that cannot be repaired: with the whole
    `replicas` parent gone, re-creating `replicas/r1` fails with `ZNONODE`, so the locks can only
    disappear because they were explicitly released.
    """
    admin = k.get_admin_client(kafka_cluster)
    topic_name = "zk_registration_loss_locks_topic"
    num_partitions = 4
    keeper_path = "/clickhouse/test/zk_registration_loss_locks"

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

        messages = [json.dumps({"key": i, "value": i}) for i in range(num_partitions)]
        k.kafka_produce(kafka_cluster, topic_name, messages, retries=5)

        base = f"{keeper_path}/topic_partition_locks"
        expected_locks = {f"{topic_name}_{pid}.lock" for pid in range(num_partitions)}
        wait_for_locks(kafka_cluster, base, expected_locks, "r1")

        instance.query_with_retry(
            "SELECT count() FROM test.view",
            check_callback=lambda res: int(res.strip()) >= len(messages),
            retry_count=60,
            sleep_time=1,
        )

        # Remove the registration of every replica behind the server's back, while the session stays alive.
        with KeeperClient.from_cluster(kafka_cluster, keeper_node="zoo1") as zk:
            zk.rmr(f"{keeper_path}/replicas")
            assert "replicas" not in set(zk.ls(keeper_path))

        # The locks this replica is not entitled to anymore have to go, without waiting for the session.
        deadline = time.time() + 180.0
        remaining = None
        while time.time() < deadline:
            with KeeperClient.from_cluster(kafka_cluster, keeper_node="zoo1") as zk:
                remaining = [lock for lock in zk.ls(base) if lock]
            if not remaining:
                break
            time.sleep(1.0)
        else:
            pytest.fail(f"Timed out waiting for the topic-partition locks to be released, still held: {remaining!r}")


def test_persistent_is_active_marker_not_counted(kafka_cluster):
    """A session-less `is_active` node must not make a dead replica look alive.

    `is_active` means "there is a live session behind this replica" only because it is ephemeral.
    A node with the same name that is not tied to a session - e.g. re-created from outside the
    server - says nothing about liveness, so counting it brings back the very quota bug this
    change fixes: the live replica would share the partitions with a replica that consumes nothing.
    """
    admin = k.get_admin_client(kafka_cluster)
    topic_name = "zk_persistent_marker_topic"
    num_partitions = 4
    keeper_path = "/clickhouse/test/zk_persistent_marker"

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
            # A replica that died without cleaning up, whose `is_active` marker was re-created as a
            # plain persistent node. `keeper-client create` makes persistent nodes by default, which
            # is exactly the out-of-band recreation we want to reject here.
            zk.create(f"{keeper_path}/replicas/ghost", "0")
            zk.create(f"{keeper_path}/replicas/ghost/is_active", "1")

        ghost_marker = f"{keeper_path}/replicas/ghost/is_active"
        kazoo = kafka_cluster.get_kazoo_client("zoo1")
        try:
            # The planted marker has to be the kind of node this test is about: not tied to any
            # session, so it outlives the client that created it.
            assert kazoo.exists(ghost_marker).ephemeralOwner == 0
        finally:
            kazoo.stop()
            kazoo.close()

        # Now start consuming: the first lock assignment happens with the ghost replica present.
        instance.query(
            "CREATE MATERIALIZED VIEW test.consumer TO test.view AS SELECT * FROM test.kafka"
        )

        messages = [json.dumps({"key": i, "value": i}) for i in range(2 * num_partitions)]
        k.kafka_produce(kafka_cluster, topic_name, messages, retries=5)

        base = f"{keeper_path}/topic_partition_locks"
        expected_locks = {f"{topic_name}_{pid}.lock" for pid in range(num_partitions)}

        # `r1` is the only live replica, so it must own every partition. If the persistent marker
        # were counted, the node quota would be `num_partitions / 2` and `r1` would never own the
        # whole lock set.
        wait_for_locks(kafka_cluster, base, expected_locks, "r1")

        instance.query_with_retry(
            "SELECT count() FROM test.view",
            check_callback=lambda res: int(res.strip()) >= len(messages),
            retry_count=60,
            sleep_time=1,
        )

        # And our own registration must still be considered valid: the repair in `activate` must
        # not tear down a healthy replica because of a foreign node.
        kazoo = kafka_cluster.get_kazoo_client("zoo1")
        try:
            assert kazoo.exists(f"{keeper_path}/replicas/r1/is_active").ephemeralOwner != 0
            assert kazoo.exists(ghost_marker).ephemeralOwner == 0
        finally:
            kazoo.stop()
            kazoo.close()


def test_foreign_is_active_marker_triggers_reactivation(kafka_cluster):
    """An ephemeral `is_active` node of another Keeper session is not this replica's registration.

    `is_active` proves liveness because it is ephemeral, but it proves *whose* liveness only through
    its payload: the server stores its own identifier in it. When another client holds a node under
    our replica name, the peers account for that client, not for us, so we must neither count it as
    our own liveness signal nor keep the partition locks we took on the strength of our registration.
    We also must not steal a live session's node: whoever created it claims to be this replica, and
    two servers consuming under one name would be worse than one of them waiting. The replica has to
    release its locks, stay out while the foreign node is there, and re-register under its own
    identifier as soon as the foreign session is gone.
    """
    admin = k.get_admin_client(kafka_cluster)
    topic_name = "zk_foreign_marker_topic"
    num_partitions = 2
    keeper_path = "/clickhouse/test/zk_foreign_marker"

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

        messages = [json.dumps({"key": i, "value": i}) for i in range(num_partitions)]
        k.kafka_produce(kafka_cluster, topic_name, messages, retries=5)

        base = f"{keeper_path}/topic_partition_locks"
        expected_locks = {f"{topic_name}_{pid}.lock" for pid in range(num_partitions)}
        wait_for_locks(kafka_cluster, base, expected_locks, "r1")

        instance.query_with_retry(
            "SELECT count() FROM test.view",
            check_callback=lambda res: int(res.strip()) >= len(messages),
            retry_count=60,
            sleep_time=1,
        )

        is_active_path = f"{keeper_path}/replicas/r1/is_active"
        foreign_data = b"some-other-server"

        # The foreign node lives as long as this kazoo session does, so the session is kept open until
        # the replica has proven that it gives way to it.
        kazoo = kafka_cluster.get_kazoo_client("zoo1")
        try:
            own_data, own_stat = kazoo.get(is_active_path)
            assert own_stat.ephemeralOwner != 0
            assert own_data != foreign_data

            # Replace our node with the foreign one in a single transaction, so there is no moment
            # without a node in which the server could simply re-create its own.
            transaction = kazoo.transaction()
            transaction.delete(is_active_path, version=own_stat.version)
            transaction.create(is_active_path, foreign_data, ephemeral=True)
            results = transaction.commit()
            assert not any(isinstance(result, Exception) for result in results), results

            foreign_stat = kazoo.exists(is_active_path)
            assert foreign_stat.ephemeralOwner != 0
            assert foreign_stat.ephemeralOwner != own_stat.ephemeralOwner

            # The locks this replica is not entitled to anymore have to go, without waiting for any
            # session to expire: neither ours (it is fine) nor the foreign one (it stays alive).
            deadline = time.time() + 180.0
            remaining = None
            while time.time() < deadline:
                with KeeperClient.from_cluster(kafka_cluster, keeper_node="zoo1") as zk:
                    remaining = [lock for lock in zk.ls(base) if lock]
                if not remaining:
                    break
                time.sleep(1.0)
            else:
                pytest.fail(
                    f"Timed out waiting for the topic-partition locks to be released while {is_active_path} "
                    f"is owned by another session, still held: {remaining!r}"
                )

            # And the foreign node must be left alone: the replica waits for it to disappear rather
            # than removing a node it does not own.
            current_data, current_stat = kazoo.get(is_active_path)
            assert current_data == foreign_data
            assert current_stat.ephemeralOwner == foreign_stat.ephemeralOwner
        finally:
            # Ends the foreign session, which takes its ephemeral node with it.
            kazoo.stop()
            kazoo.close()

        # Now the replica must re-register under its own identifier ...
        deadline = time.time() + 180.0
        while time.time() < deadline:
            kazoo = kafka_cluster.get_kazoo_client("zoo1")
            try:
                if kazoo.exists(is_active_path) is not None:
                    restored_data, restored_stat = kazoo.get(is_active_path)
                    if restored_data == own_data and restored_stat.ephemeralOwner != 0:
                        break
            finally:
                kazoo.stop()
                kazoo.close()
            time.sleep(1.0)
        else:
            pytest.fail(f"Timed out waiting for {is_active_path} to be re-created by the server")

        # ... and keep consuming afterwards, with the full lock set back in place.
        k.kafka_produce(kafka_cluster, topic_name, messages, retries=5)
        wait_for_locks(kafka_cluster, base, expected_locks, "r1", timeout=120.0)

        instance.query_with_retry(
            "SELECT count() FROM test.view",
            check_callback=lambda res: int(res.strip()) >= 2 * len(messages),
            retry_count=120,
            sleep_time=1,
        )


def test_direct_read_triggers_reactivation(kafka_cluster):
    """A direct `SELECT` must not hide a lost registration behind an empty result.

    Without a materialized view there is no streaming cycle to notice that this replica is not
    among the active ones anymore, so the direct read is the only entry point that can ask for the
    reactivation. Returning an empty chunk instead would look exactly like "the topic is empty"
    and leave the table broken until the next periodic activation check, a minute later.
    """
    admin = k.get_admin_client(kafka_cluster)
    topic_name = "zk_direct_read_reactivation_topic"
    num_partitions = 2
    keeper_path = "/clickhouse/test/zk_direct_read_reactivation"

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
        )
        # No materialized view on purpose: the table is only ever read directly.
        instance.query(
            f"""
            DROP TABLE IF EXISTS test.kafka;

            {create_kafka};
            """
        )

        messages = [json.dumps({"key": i, "value": i}) for i in range(num_partitions)]
        k.kafka_produce(kafka_cluster, topic_name, messages, retries=5)

        # A healthy direct read works and does not disturb the registration.
        instance.query_with_retry(
            "SELECT count() FROM test.kafka",
            check_callback=lambda res: int(res.strip()) > 0,
            retry_count=60,
            sleep_time=1,
        )

        # Drop our own registration behind the server's back.
        is_active_path = f"{keeper_path}/replicas/r1/is_active"
        with KeeperClient.from_cluster(kafka_cluster, keeper_node="zoo1") as zk:
            zk.rm(is_active_path)
            assert "is_active" not in set(zk.ls(f"{keeper_path}/replicas/r1"))

        # A consumer keeps its current assignment for `LOCKS_REFRESH_POLLS` polls before it looks at
        # the lock distribution again, and a direct read polls only once, so it takes a handful of
        # reads until the check runs. The periodic activation check can re-create the marker in the
        # meantime, so plant the loss again whenever that happens - the read has to fail eventually
        # instead of pretending there is nothing to read.
        error = ""
        for _ in range(60):
            try:
                instance.query("SELECT count() FROM test.kafka")
            except Exception as e:  # noqa: BLE001 - the error text is what is under test
                error = str(e)
                break

            with KeeperClient.from_cluster(kafka_cluster, keeper_node="zoo1") as zk:
                if "is_active" in set(zk.ls(f"{keeper_path}/replicas/r1")):
                    zk.rm(is_active_path)
        assert "ABORTED" in error, (
            f"the direct read never failed while the registration was lost: {error!r}"
        )

        # ... and it must have scheduled the reactivation right away. The periodic activation check
        # runs only once a minute, so a marker that is back well before that can only come from the
        # reactivation the direct read asked for.
        deadline = time.time() + 20.0
        while time.time() < deadline:
            with KeeperClient.from_cluster(kafka_cluster, keeper_node="zoo1") as zk:
                if "is_active" in set(zk.ls(f"{keeper_path}/replicas/r1")):
                    break
            time.sleep(0.5)
        else:
            pytest.fail(
                f"Timed out waiting for {is_active_path} to be re-created: the direct read did not "
                "schedule the reactivation"
            )

        # And reads must work again afterwards.
        k.kafka_produce(kafka_cluster, topic_name, messages, retries=5)
        instance.query_with_retry(
            "SELECT count() FROM test.kafka",
            check_callback=lambda res: int(res.strip()) > 0,
            retry_count=120,
            sleep_time=1,
        )


def test_keeper_session_loss_triggers_reactivation(kafka_cluster):
    """An expired Keeper session must be recovered as soon as a consumer runs into it.

    The activating task checks the session only once a minute. Until it does, the table still
    counts as active, so a consumer that finds its session expired used to stall the stream, and a
    direct read used to return nothing, for up to a minute: the topic looked empty, while every
    ephemeral node of the replica, `is_active` included, was already gone. The consumer that notices
    the expiry has to ask for the reactivation right away, and a direct read has to fail instead of
    pretending there is nothing to read.
    """
    admin = k.get_admin_client(kafka_cluster)
    topic_name = "zk_session_loss_reactivation_topic"
    num_partitions = 2
    keeper_path = "/clickhouse/test/zk_session_loss_reactivation"

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
        )
        # No materialized view on purpose: the table is only ever read directly, so the direct read is
        # the only thing that can notice the expired session before the periodic check does.
        instance.query(
            f"""
            DROP TABLE IF EXISTS test.kafka;

            {create_kafka};
            """
        )

        messages = [json.dumps({"key": i, "value": i}) for i in range(num_partitions)]
        k.kafka_produce(kafka_cluster, topic_name, messages, retries=5)

        instance.query_with_retry(
            "SELECT count() FROM test.kafka",
            check_callback=lambda res: int(res.strip()) > 0,
            retry_count=60,
            sleep_time=1,
        )

        # Phase the partition on the periodic activation check. If that check ran while the session
        # is cut off, it would deactivate the table itself and its own retry loop would bring it back
        # once the network is restored, so the consumer's reaction would not be what recovers the
        # table. Waiting for the check to run leaves the whole minute until the next one, and the
        # session expires well within it.
        check_marker = "StorageKafka2 (test.kafka .*No need to activate"
        checks_before = int(instance.count_in_log(check_marker))
        deadline = time.time() + 90.0
        while time.time() < deadline:
            if int(instance.count_in_log(check_marker)) > checks_before:
                break
            time.sleep(0.5)
        else:
            pytest.fail("Timed out waiting for the periodic activation check to run")

        is_active_path = f"{keeper_path}/replicas/r1/is_active"
        with PartitionManager() as pm:
            pm.drop_instance_zk_connections(instance)
            # Keeper drops the ephemeral nodes of the session once it expires. That is the moment
            # from which the peers do not count this replica anymore, and it is also when Keeper
            # closes the connection, so the server side of the session is gone too.
            deadline = time.time() + 120.0
            while time.time() < deadline:
                with KeeperClient.from_cluster(kafka_cluster, keeper_node="zoo1") as zk:
                    if "is_active" not in set(zk.ls(f"{keeper_path}/replicas/r1")):
                        break
                time.sleep(1.0)
            else:
                pytest.fail(f"Timed out waiting for Keeper to expire the session owning {is_active_path}")

        # The network is back, but nothing has told the table about the expired session yet. The
        # direct read has to notice it and fail, rather than pretend the topic is empty. Right after
        # the partition the consumer can still run into the closing of the old connection, which
        # surfaces as an ordinary Keeper error, so give it a few reads.
        error = ""
        for _ in range(30):
            try:
                result = instance.query("SELECT count() FROM test.kafka")
            except Exception as e:  # noqa: BLE001 - the error text is what is under test
                error = str(e)
                if "ABORTED" in error:
                    break
                time.sleep(1.0)
                continue
            pytest.fail(
                f"the direct read returned {result.strip()!r} while the Keeper session was expired, "
                "instead of failing and asking for the reactivation"
            )
        assert "ABORTED" in error, f"the direct read did not fail with ABORTED: {error!r}"
        assert "Keeper session has expired" in error, error

        # ... and it must have scheduled the reactivation right away. The periodic activation check
        # runs only once a minute and has just run, so a marker that is back well before that can
        # only come from the reactivation the direct read asked for.
        deadline = time.time() + 20.0
        while time.time() < deadline:
            with KeeperClient.from_cluster(kafka_cluster, keeper_node="zoo1") as zk:
                if "is_active" in set(zk.ls(f"{keeper_path}/replicas/r1")):
                    break
            time.sleep(0.5)
        else:
            pytest.fail(
                f"Timed out waiting for {is_active_path} to be re-created: the direct read did not "
                "schedule the reactivation"
            )

        # And reads must work again afterwards.
        k.kafka_produce(kafka_cluster, topic_name, messages, retries=5)
        instance.query_with_retry(
            "SELECT count() FROM test.kafka",
            check_callback=lambda res: int(res.strip()) > 0,
            retry_count=120,
            sleep_time=1,
        )


def test_replayed_is_active_payload_triggers_reactivation(kafka_cluster):
    """A foreign ephemeral `is_active` node carrying this replica's own identifier is still foreign.

    The identifier the server stores in its `is_active` node is readable from Keeper, so another client
    can delete the node and re-create it as its own ephemeral node with the very same payload. Judged by
    the payload alone, both the replica and its consumers would take that node for their own registration
    and keep consuming, while the peers count the foreign client under our name; once our real session
    dies later, the replica stays counted as live and the quota bug returns. Ownership must therefore be
    tied to the Keeper session that created the node: the replica has to release its locks, leave the
    foreign node alone, and re-register only once the foreign session is gone.
    """
    admin = k.get_admin_client(kafka_cluster)
    topic_name = "zk_replayed_marker_topic"
    num_partitions = 2
    keeper_path = "/clickhouse/test/zk_replayed_marker"

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

        messages = [json.dumps({"key": i, "value": i}) for i in range(num_partitions)]
        k.kafka_produce(kafka_cluster, topic_name, messages, retries=5)

        base = f"{keeper_path}/topic_partition_locks"
        expected_locks = {f"{topic_name}_{pid}.lock" for pid in range(num_partitions)}
        wait_for_locks(kafka_cluster, base, expected_locks, "r1")

        instance.query_with_retry(
            "SELECT count() FROM test.view",
            check_callback=lambda res: int(res.strip()) >= len(messages),
            retry_count=60,
            sleep_time=1,
        )

        is_active_path = f"{keeper_path}/replicas/r1/is_active"

        # The replayed node lives as long as this kazoo session does, so the session is kept open until
        # the replica has proven that it gives way to it.
        kazoo = kafka_cluster.get_kazoo_client("zoo1")
        try:
            own_data, own_stat = kazoo.get(is_active_path)
            assert own_stat.ephemeralOwner != 0

            # Replace our node with one of this session that carries exactly our payload, in a single
            # transaction so there is no moment without a node in which the server could re-create its own.
            transaction = kazoo.transaction()
            transaction.delete(is_active_path, version=own_stat.version)
            transaction.create(is_active_path, own_data, ephemeral=True)
            results = transaction.commit()
            assert not any(isinstance(result, Exception) for result in results), results

            replayed_stat = kazoo.exists(is_active_path)
            assert replayed_stat.ephemeralOwner != 0
            assert replayed_stat.ephemeralOwner != own_stat.ephemeralOwner

            # Only the owner session tells this node apart from ours, and it is enough: the locks have to
            # go without waiting for any session to expire.
            deadline = time.time() + 180.0
            remaining = None
            while time.time() < deadline:
                with KeeperClient.from_cluster(kafka_cluster, keeper_node="zoo1") as zk:
                    remaining = [lock for lock in zk.ls(base) if lock]
                if not remaining:
                    break
                time.sleep(1.0)
            else:
                pytest.fail(
                    f"Timed out waiting for the topic-partition locks to be released while {is_active_path} "
                    f"is owned by another session with the same payload, still held: {remaining!r}"
                )

            # The replayed node must be left alone: its payload does not make it ours to remove.
            current_data, current_stat = kazoo.get(is_active_path)
            assert current_data == own_data
            assert current_stat.ephemeralOwner == replayed_stat.ephemeralOwner
        finally:
            # Ends the foreign session, which takes its ephemeral node with it.
            kazoo.stop()
            kazoo.close()

        # Now the replica must re-register with a node of its own session ...
        deadline = time.time() + 180.0
        while time.time() < deadline:
            kazoo = kafka_cluster.get_kazoo_client("zoo1")
            try:
                if kazoo.exists(is_active_path) is not None:
                    restored_data, restored_stat = kazoo.get(is_active_path)
                    if (
                        restored_data == own_data
                        and restored_stat.ephemeralOwner != 0
                        and restored_stat.ephemeralOwner != replayed_stat.ephemeralOwner
                    ):
                        break
            finally:
                kazoo.stop()
                kazoo.close()
            time.sleep(1.0)
        else:
            pytest.fail(f"Timed out waiting for {is_active_path} to be re-created by the server")

        # ... and keep consuming afterwards, with the full lock set back in place.
        k.kafka_produce(kafka_cluster, topic_name, messages, retries=5)
        wait_for_locks(kafka_cluster, base, expected_locks, "r1", timeout=120.0)

        instance.query_with_retry(
            "SELECT count() FROM test.view",
            check_callback=lambda res: int(res.strip()) >= 2 * len(messages),
            retry_count=120,
            sleep_time=1,
        )


def check_marker_planted_during_reactivation_is_kept(kafka_cluster, failpoint, topic_name, keeper_path):
    """Plant a foreign same-payload `is_active` node while the reactivation is paused at `failpoint`.

    The reactivation path validates `replicas/<self>/is_active` (payload and owner session) before it
    removes what it takes for its own leftover. A node that another Keeper client creates *after* that
    validation must survive both removals: the old `EphemeralNodeHolder` dropped by `partialShutdown`
    and the leftover removal right before the server re-creates the node. Both used to remove by path
    or payload alone, so the foreign live node was deleted and the replica registered over it, which is
    exactly the split-brain the ownership checks are there to prevent. The removals have to be bound to
    the version that was validated instead, and the replica has to stay out until the foreign node is
    gone.
    """
    admin = k.get_admin_client(kafka_cluster)
    num_partitions = 2

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

        messages = [json.dumps({"key": i, "value": i}) for i in range(num_partitions)]
        k.kafka_produce(kafka_cluster, topic_name, messages, retries=5)

        base = f"{keeper_path}/topic_partition_locks"
        expected_locks = {f"{topic_name}_{pid}.lock" for pid in range(num_partitions)}
        wait_for_locks(kafka_cluster, base, expected_locks, "r1")

        instance.query_with_retry(
            "SELECT count() FROM test.view",
            check_callback=lambda res: int(res.strip()) >= len(messages),
            retry_count=60,
            sleep_time=1,
        )

        is_active_path = f"{keeper_path}/replicas/r1/is_active"
        conflict_line = "appears to be already active"
        conflicts_before = int(instance.count_in_log(conflict_line))

        # The planted node lives as long as this kazoo session does, so the session is kept open until
        # the replica has proven that it gives way to it.
        kazoo = kafka_cluster.get_kazoo_client("zoo1")
        try:
            own_data, own_stat = kazoo.get(is_active_path)
            assert own_stat.ephemeralOwner != 0

            instance.query(f"SYSTEM ENABLE FAILPOINT {failpoint}")
            try:
                # Drop our own registration behind the server's back: the consumers notice that this
                # replica is not active anymore and hand over to the reactivation, which stops at the
                # failpoint right after it has checked what is (not) at the `is_active` path.
                kazoo.delete(is_active_path, version=own_stat.version)
                instance.query(f"SYSTEM WAIT FAILPOINT {failpoint} PAUSE", timeout=240)

                # Now another client registers under our name with our very payload.
                kazoo.create(is_active_path, own_data, ephemeral=True)
                planted_stat = kazoo.exists(is_active_path)
                assert planted_stat.ephemeralOwner != 0
                assert planted_stat.ephemeralOwner != own_stat.ephemeralOwner
            finally:
                instance.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")

            # The reactivation goes on and must report the conflict instead of removing the planted node.
            deadline = time.time() + 120.0
            while time.time() < deadline:
                current_stat = kazoo.exists(is_active_path)
                if current_stat is None or current_stat.ephemeralOwner != planted_stat.ephemeralOwner:
                    pytest.fail(
                        f"The server took over {is_active_path} planted by another session (now: {current_stat!r})"
                    )
                if int(instance.count_in_log(conflict_line)) > conflicts_before:
                    break
                time.sleep(1.0)
            else:
                pytest.fail(f"Timed out waiting for the server to report that {is_active_path} is foreign")

            # The locks have to go too: the peers do not count this replica while the node is foreign.
            deadline = time.time() + 120.0
            remaining = None
            while time.time() < deadline:
                with KeeperClient.from_cluster(kafka_cluster, keeper_node="zoo1") as zk:
                    remaining = [lock for lock in zk.ls(base) if lock]
                if not remaining:
                    break
                time.sleep(1.0)
            else:
                pytest.fail(f"Timed out waiting for the topic-partition locks to be released, still held: {remaining!r}")

            current_data, current_stat = kazoo.get(is_active_path)
            assert current_data == own_data
            assert current_stat.ephemeralOwner == planted_stat.ephemeralOwner
        finally:
            # Ends the foreign session, which takes its ephemeral node with it.
            kazoo.stop()
            kazoo.close()

        # Now the replica must re-register with a node of its own session ...
        deadline = time.time() + 180.0
        while time.time() < deadline:
            kazoo = kafka_cluster.get_kazoo_client("zoo1")
            try:
                if kazoo.exists(is_active_path) is not None:
                    restored_data, restored_stat = kazoo.get(is_active_path)
                    if (
                        restored_data == own_data
                        and restored_stat.ephemeralOwner != 0
                        and restored_stat.ephemeralOwner != planted_stat.ephemeralOwner
                    ):
                        break
            finally:
                kazoo.stop()
                kazoo.close()
            time.sleep(1.0)
        else:
            pytest.fail(f"Timed out waiting for {is_active_path} to be re-created by the server")

        # ... and keep consuming afterwards, with the full lock set back in place.
        k.kafka_produce(kafka_cluster, topic_name, messages, retries=5)
        wait_for_locks(kafka_cluster, base, expected_locks, "r1", timeout=120.0)

        instance.query_with_retry(
            "SELECT count() FROM test.view",
            check_callback=lambda res: int(res.strip()) >= 2 * len(messages),
            retry_count=120,
            sleep_time=1,
        )


def test_marker_planted_before_partial_shutdown_is_kept(kafka_cluster):
    """A foreign node that appears after the pre-check and before `partialShutdown` must not be removed by
    the old node holder, which knows nothing but the path."""
    check_marker_planted_during_reactivation_is_kept(
        kafka_cluster,
        "kafka2_activate_pause_before_partial_shutdown",
        "zk_marker_before_shutdown_topic",
        "/clickhouse/test/zk_marker_before_shutdown",
    )


def test_marker_planted_before_leftover_removal_is_kept(kafka_cluster):
    """A foreign node that appears after the ownership check and before the leftover removal in the
    re-registration must not be removed by its payload."""
    check_marker_planted_during_reactivation_is_kept(
        kafka_cluster,
        "kafka2_activate_pause_before_is_active_removal",
        "zk_marker_before_removal_topic",
        "/clickhouse/test/zk_marker_before_removal",
    )
