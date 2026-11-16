import concurrent
import time
import uuid

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.network import PartitionManager
from helpers.test_tools import TSV

cluster = ClickHouseCluster(__file__)

zero = cluster.add_instance(
    "zero",
    user_configs=["configs/users.d/settings.xml"],
    main_configs=[
        "configs/config.d/remote_servers.xml",
        "configs/config.d/storage_conf.xml",
    ],
    macros={"cluster": "anime", "shard": "0", "replica": "zero"},
    with_zookeeper=True,
    with_minio=True,
)

first = cluster.add_instance(
    "first",
    user_configs=["configs/users.d/settings.xml"],
    main_configs=[
        "configs/config.d/remote_servers.xml",
        "configs/config.d/storage_conf.xml",
    ],
    macros={"cluster": "anime", "shard": "0", "replica": "first"},
    with_zookeeper=True,
    with_minio=True,
)

second = cluster.add_instance(
    "second",
    user_configs=["configs/users.d/settings.xml"],
    main_configs=[
        "configs/config.d/remote_servers.xml",
        "configs/config.d/storage_conf.xml",
    ],
    macros={"cluster": "anime", "shard": "0", "replica": "second"},
    with_zookeeper=True,
    with_minio=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    global cluster
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_simple_add_replica(started_cluster):
    table_name = "test_simple_" + uuid.uuid4().hex

    create_query = (
        f"CREATE TABLE {table_name} "
        "(a Int8, d Date) "
        "Engine = ReplicatedMergeTree('/clickhouse/tables/{shard}/{table}', '{replica}') "
        "PARTITION BY d ORDER BY a"
    )

    zero.query(create_query)
    first.query(create_query)

    first.query(f"SYSTEM STOP FETCHES {table_name}")

    zero.query(
        f"INSERT INTO {table_name} VALUES (1, '2011-01-01')",
        settings={"insert_quorum": 1},
    )

    assert "1\t2011-01-01\n" == zero.query(f"SELECT * from {table_name}")
    assert "" == first.query(f"SELECT * from {table_name}")

    first.query(f"SYSTEM START FETCHES {table_name}")

    first.query(f"SYSTEM SYNC REPLICA {table_name}", timeout=20)

    assert "1\t2011-01-01\n" == zero.query(f"SELECT * from {table_name}")
    assert "1\t2011-01-01\n" == first.query(f"SELECT * from {table_name}")

    second.query(create_query)
    second.query(f"SYSTEM SYNC REPLICA {table_name}", timeout=20)

    assert "1\t2011-01-01\n" == zero.query(f"SELECT * from {table_name}")
    assert "1\t2011-01-01\n" == first.query(f"SELECT * from {table_name}")
    assert "1\t2011-01-01\n" == second.query(f"SELECT * from {table_name}")

    zero.query(f"DROP TABLE IF EXISTS {table_name} ON CLUSTER cluster")


def test_drop_replica_and_achieve_quorum(started_cluster):
    table_name = "test_drop_replica_and_achieve_quorum_" + uuid.uuid4().hex
    create_query = (
        f"CREATE TABLE {table_name} "
        "(a Int8, d Date) "
        "Engine = ReplicatedMergeTree('/clickhouse/tables/{shard}/{table}', '{replica}') "
        "PARTITION BY d ORDER BY a"
    )
    print("Create Replicated table with two replicas")
    zero.query(create_query)
    first.query(create_query)
    print("Stop fetches on one replica. Since that, it will be isolated.")
    first.query(f"SYSTEM STOP FETCHES {table_name}")
    print("Insert to other replica. This query will fail.")
    quorum_timeout = zero.query_and_get_error(
        f"INSERT INTO {table_name}(a,d) VALUES (1, '2011-01-01')",
        settings={"insert_quorum_timeout": 5000},
    )
    assert "Timeout while waiting for quorum" in quorum_timeout, "Query must fail."
    assert TSV("1\t2011-01-01\n") == TSV(
        zero.query(
            f"SELECT * FROM {table_name}",
            settings={"select_sequential_consistency": 0},
        )
    )
    assert TSV("") == TSV(
        zero.query(
            f"SELECT * FROM {table_name}",
            settings={"select_sequential_consistency": 1},
        )
    )
    # TODO:(Mikhaylov) begin; maybe delete this lines. I want clickhouse to fetch parts and update quorum.
    print("START FETCHES first replica")
    first.query(f"SYSTEM START FETCHES {table_name}")
    print("SYNC first replica")
    first.query(f"SYSTEM SYNC REPLICA {table_name}", timeout=20)
    # TODO:(Mikhaylov) end
    print("Add second replica")
    second.query(create_query)
    print("SYNC second replica")
    second.query(f"SYSTEM SYNC REPLICA {table_name}", timeout=20)
    print("Quorum for previous insert achieved.")
    assert TSV("1\t2011-01-01\n") == TSV(
        second.query(
            f"SELECT * FROM {table_name}",
            settings={"select_sequential_consistency": 1},
        )
    )


@pytest.mark.parametrize(("add_new_data"), [False, True])
def test_insert_quorum_with_drop_partition(started_cluster, add_new_data):
    # use different table names for easier disambiguation in logs between runs (you may also check uuid though, but not always convenient)
    table_name = (
        "test_quorum_insert_with_drop_partition_new_data"
        if add_new_data
        else "test_quorum_insert_with_drop_partition"
    ) + uuid.uuid4().hex
    zero.query(f"DROP TABLE IF EXISTS {table_name} ON CLUSTER cluster")

    create_query = (
        f"CREATE TABLE {table_name} ON CLUSTER cluster "
        "(a Int8, d Date) "
        "Engine = ReplicatedMergeTree "
        "PARTITION BY d ORDER BY a "
    )

    print("Create Replicated table with three replicas")
    zero.query(create_query)

    print(f"Stop fetches for {table_name} at first replica.")
    first.query(f"SYSTEM STOP FETCHES {table_name}")

    print("Insert with quorum. (zero and second)")
    zero.query(f"INSERT INTO {table_name}(a,d) VALUES(1, '2011-01-01')")

    print("Drop partition.")
    zero.query(f"ALTER TABLE {table_name} DROP PARTITION '2011-01-01'")

    if add_new_data:
        print("Insert to deleted partition")
        zero.query(f"INSERT INTO {table_name}(a,d) VALUES(2, '2011-01-01')")

    print(f"Resume fetches for {table_name} at first replica.")
    first.query(f"SYSTEM START FETCHES {table_name}")

    print("Sync first replica with others.")
    first.query(f"SYSTEM SYNC REPLICA {table_name}")

    assert "20110101" not in first.query(
        f"""
    WITH (SELECT toString(uuid) FROM system.tables WHERE name = '{table_name}') AS uuid,
         '/clickhouse/tables/' || uuid || '/0/quorum/last_part' AS p
    SELECT * FROM system.zookeeper WHERE path = p FORMAT Vertical
    """
    )

    # Sync second replica not to have `REPLICA_IS_NOT_IN_QUORUM` error
    second.query(f"SYSTEM SYNC REPLICA {table_name}")

    print("Select from updated partition.")
    if add_new_data:
        assert TSV("2\t2011-01-01\n") == TSV(zero.query(f"SELECT * FROM {table_name}"))
        assert TSV("2\t2011-01-01\n") == TSV(
            second.query(f"SELECT * FROM {table_name}")
        )
    else:
        # Use select_sequential_consistency=0 because after DROP PARTITION the /quorum/last_part
        # node is cleaned, but there's a race condition where updateQuorum could re-add the entry
        # if it was retrying due to a concurrent modification.
        # Since we're just checking that the partition is empty (not reading consistent data),
        # we don't need sequential consistency here.
        assert TSV("") == TSV(
            zero.query(
                f"SELECT * FROM {table_name}",
                settings={"select_sequential_consistency": 0},
            )
        )
        assert TSV("") == TSV(
            second.query(
                f"SELECT * FROM {table_name}",
                settings={"select_sequential_consistency": 0},
            )
        )

    zero.query(f"DROP TABLE IF EXISTS {table_name} ON CLUSTER cluster")


@pytest.mark.parametrize(("add_new_data"), [False, True])
def test_insert_quorum_with_move_partition(started_cluster, add_new_data):
    # use different table names for easier disambiguation in logs between runs (you may also check uuid though, but not always convenient)
    source_table_name = (
        "test_insert_quorum_with_move_partition_source_new_data"
        if add_new_data
        else "test_insert_quorum_with_move_partition_source"
    ) + uuid.uuid4().hex
    destination_table_name = (
        "test_insert_quorum_with_move_partition_destination_new_data"
        if add_new_data
        else "test_insert_quorum_with_move_partition_destination"
    ) + uuid.uuid4().hex
    zero.query(f"DROP TABLE IF EXISTS {source_table_name} ON CLUSTER cluster")
    zero.query(f"DROP TABLE IF EXISTS {destination_table_name} ON CLUSTER cluster")

    create_source = (
        f"CREATE TABLE {source_table_name} ON CLUSTER cluster "
        "(a Int8, d Date) "
        "Engine = ReplicatedMergeTree "
        "PARTITION BY d ORDER BY a "
    )

    create_destination = (
        f"CREATE TABLE {destination_table_name} ON CLUSTER cluster "
        "(a Int8, d Date) "
        "Engine = ReplicatedMergeTree "
        "PARTITION BY d ORDER BY a "
    )

    print("Create source Replicated table with three replicas")
    zero.query(create_source)

    print("Create destination Replicated table with three replicas")
    zero.query(create_destination)

    print(f"Stop fetches for {source_table_name} at first replica.")
    first.query(f"SYSTEM STOP FETCHES {source_table_name}")

    print("Insert with quorum. (zero and second)")
    zero.query(f"INSERT INTO {source_table_name}(a,d) VALUES(1, '2011-01-01')")

    print("Drop partition.")
    zero.query(
        f"ALTER TABLE {source_table_name} MOVE PARTITION '2011-01-01' TO TABLE {destination_table_name}"
    )

    if add_new_data:
        print("Insert to deleted partition")
        zero.query(f"INSERT INTO {source_table_name}(a,d) VALUES(2, '2011-01-01')")

    print(f"Resume fetches for {source_table_name} at first replica.")
    first.query(f"SYSTEM START FETCHES {source_table_name}")

    print("Sync first replica with others.")
    first.query(f"SYSTEM SYNC REPLICA {source_table_name}")

    assert "20110101" not in first.query(
        f"""
    WITH (SELECT toString(uuid) FROM system.tables WHERE name = '{source_table_name}') AS uuid,
         '/clickhouse/tables/' || uuid || '/0/quorum/last_part' AS p
    SELECT * FROM system.zookeeper WHERE path = p FORMAT Vertical
    """
    )

    # Sync second replica not to have `REPLICA_IS_NOT_IN_QUORUM` error
    second.query(f"SYSTEM SYNC REPLICA {source_table_name}")

    print("Select from updated partition.")
    if add_new_data:
        assert TSV("2\t2011-01-01\n") == TSV(
            zero.query(f"SELECT * FROM {source_table_name}")
        )
        assert TSV("2\t2011-01-01\n") == TSV(
            second.query(f"SELECT * FROM {source_table_name}")
        )
    else:
        # Use select_sequential_consistency=0 because after MOVE PARTITION the /quorum/last_part
        # node is cleaned, but there's a race condition where updateQuorum could re-add the entry
        # if it was retrying due to a concurrent modification.
        # Since we're just checking that the partition is empty (not reading consistent data),
        # we don't need sequential consistency here.
        assert TSV("") == TSV(
            zero.query(
                f"SELECT * FROM {source_table_name}",
                settings={"select_sequential_consistency": 0},
            )
        )
        assert TSV("") == TSV(
            second.query(
                f"SELECT * FROM {source_table_name}",
                settings={"select_sequential_consistency": 0},
            )
        )

    zero.query(f"DROP TABLE IF EXISTS {source_table_name} ON CLUSTER cluster")
    zero.query(f"DROP TABLE IF EXISTS {destination_table_name} ON CLUSTER cluster")


def test_insert_quorum_with_ttl(started_cluster):
    table_name = "test_insert_quorum_with_ttl_" + uuid.uuid4().hex

    create_query = (
        f"CREATE TABLE {table_name} "
        "(a Int8, d Date) "
        "Engine = ReplicatedMergeTree('/clickhouse/tables/{table}', '{replica}') "
        "PARTITION BY d ORDER BY a "
        "TTL d + INTERVAL 5 second DELETE WHERE toYear(d) = 2011 "
        "SETTINGS merge_with_ttl_timeout=2 "
    )

    print("Create Replicated table with two replicas")
    zero.query(create_query)
    first.query(create_query)

    print(f"Stop fetches for {table_name} at first replica.")
    first.query(f"SYSTEM STOP FETCHES {table_name}")

    print("Insert should fail since it can not reach the quorum.")
    quorum_timeout = zero.query_and_get_error(
        f"INSERT INTO {table_name}(a,d) VALUES(1, '2011-01-01')",
        settings={"insert_quorum_timeout": 5000},
    )
    assert "Timeout while waiting for quorum" in quorum_timeout, "Query must fail."

    print(
        "Wait 10 seconds and TTL merge have to be executed. But it won't delete data."
    )
    time.sleep(10)
    assert TSV("1\t2011-01-01\n") == TSV(
        zero.query(
            f"SELECT * FROM {table_name}",
            settings={"select_sequential_consistency": 0},
        )
    )

    print(f"Resume fetches for {table_name} at first replica.")
    first.query(f"SYSTEM START FETCHES {table_name}")

    print("Sync first replica.")
    first.query(f"SYSTEM SYNC REPLICA {table_name}")

    zero.query(
        f"INSERT INTO {table_name}(a,d) VALUES(1, '2011-01-01')",
        settings={"insert_quorum_timeout": 5000},
    )

    print("Inserts should resume.")
    zero.query(f"INSERT INTO {table_name}(a, d) VALUES(2, '2012-02-02')")

    first.query(f"OPTIMIZE TABLE {table_name}")
    first.query(f"SYSTEM SYNC REPLICA {table_name}")
    zero.query(f"SYSTEM SYNC REPLICA {table_name}")

    assert TSV("2\t2012-02-02\n") == TSV(
        first.query(
            f"SELECT * FROM {table_name}",
            settings={"select_sequential_consistency": 0},
        )
    )
    assert TSV("2\t2012-02-02\n") == TSV(
        first.query(
            f"SELECT * FROM {table_name}",
            settings={"select_sequential_consistency": 1},
        )
    )

    zero.query(f"DROP TABLE IF EXISTS {table_name} ON CLUSTER cluster")


def test_auto_quorum_rejects_when_too_few_live_replicas(started_cluster):
    # Regression test for insert_quorum = 'auto': the majority quorum size must be
    # computed from the actual replica count (replicas_number / 2 + 1), not from a
    # not-yet-initialized member that used to leave it at 0 (giving 0 / 2 + 1 == 1,
    # which active_replicas == 1 can never undercut, silently bypassing the check).
    table_name = "test_auto_quorum_too_few_live_replicas_" + uuid.uuid4().hex
    create_query = (
        f"CREATE TABLE {table_name} "
        "(a Int8, d Date) "
        "Engine = ReplicatedMergeTree('/clickhouse/tables/{shard}/{table}', '{replica}') "
        "PARTITION BY d ORDER BY a"
    )

    # Two registered replicas -> majority quorum size is 2 / 2 + 1 == 2.
    zero.query(create_query)
    first.query(create_query)

    # Make the second replica inactive while it stays registered, so only one of the
    # two replicas is alive. Detaching removes its /is_active node but keeps it in /replicas.
    first.query(f"DETACH TABLE {table_name}")

    is_active_path = f"/clickhouse/tables/0/{table_name}/replicas/first"
    for _ in range(120):
        if (
            zero.query(
                f"SELECT count() FROM system.zookeeper "
                f"WHERE path = '{is_active_path}' AND name = 'is_active'"
            ).strip()
            == "0"
        ):
            break
        time.sleep(1)
    else:
        raise Exception("Replica 'first' did not become inactive")

    # With only one of two replicas alive, an 'auto' (majority) quorum insert must fail
    # up front, before writing any local part. A short timeout makes sure that, if the
    # regression were present, we would observe a quorum timeout instead of this error.
    error = zero.query_and_get_error(
        f"INSERT INTO {table_name}(a,d) VALUES (1, '2011-01-01')",
        settings={"insert_quorum": "auto", "insert_quorum_timeout": 5000},
    )
    assert "TOO_FEW_LIVE_REPLICAS" in error, error
    assert "Number of alive replicas (1) is less than requested quorum (2/2)" in error, error

    # The insert must have failed before committing anything locally ...
    assert TSV("") == TSV(
        zero.query(
            f"SELECT * FROM {table_name}",
            settings={"select_sequential_consistency": 0},
        )
    )

    # ... and before creating a quorum/status node, so it cannot poison the next insert
    # with UNSATISFIED_QUORUM_FOR_PREVIOUS_WRITE.
    assert "0" == zero.query(
        f"SELECT count() FROM system.zookeeper "
        f"WHERE path = '/clickhouse/tables/0/{table_name}/quorum' AND name = 'status'"
    ).strip()

    # Once the second replica is back, a majority is available again and the insert succeeds.
    first.query(f"ATTACH TABLE {table_name}")
    first.query(f"SYSTEM SYNC REPLICA {table_name}", timeout=20)
    zero.query(
        f"INSERT INTO {table_name}(a,d) VALUES (1, '2011-01-01')",
        settings={"insert_quorum": "auto", "insert_quorum_timeout": 20000},
    )
    assert TSV("1\t2011-01-01\n") == TSV(zero.query(f"SELECT * FROM {table_name}"))

    zero.query(f"DROP TABLE IF EXISTS {table_name} ON CLUSTER cluster")


def test_insert_quorum_with_keeper_loss_connection(started_cluster):
    table_name = "test_insert_quorum_with_keeper_loss_" + uuid.uuid4().hex
    create_query = (
        f"CREATE TABLE {table_name} "
        "(a Int8, d Date) "
        "Engine = ReplicatedMergeTree('/clickhouse/tables/{table}', '{replica}') "
        "ORDER BY a "
    )

    zero.query(create_query)
    first.query(create_query)

    first.query(f"SYSTEM STOP FETCHES {table_name}")

    zero.query("SYSTEM ENABLE FAILPOINT replicated_merge_tree_commit_zk_fail_after_op")
    zero.query("SYSTEM ENABLE FAILPOINT replicated_merge_tree_insert_retry_pause")

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        insert_future = executor.submit(
            lambda: zero.query(
                f"INSERT INTO {table_name}(a,d) VALUES(1, '2011-01-01')",
                settings={"insert_quorum_timeout": 150000},
            )
        )

        zk = cluster.get_kazoo_client("zoo1")

        # Ensure that part had been committed
        retries = 0
        while True:
            if zk.exists(
                f"/clickhouse/tables/{table_name}/replicas/zero/parts/all_0_0_0"
            ):
                break
            print("replica still did not create all_0_0_0")
            time.sleep(1)
            retries += 1
            if retries == 120:
                raise Exception("Can not wait for all_0_0_0 part")

        with PartitionManager() as pm:
            pm.drop_instance_zk_connections(zero)

            retries = 0
            while True:
                if (
                    zk.exists(
                        f"/clickhouse/tables/{table_name}/replicas/zero/is_active"
                    )
                    is None
                ):
                    break
                print("replica is still active")
                time.sleep(1)
                retries += 1
                if retries == 120:
                    raise Exception("Can not wait cluster replica inactive")

            first.query("SYSTEM ENABLE FAILPOINT finish_set_quorum_failed_parts")
            quorum_fail_future = executor.submit(
                lambda: first.query(
                    "SYSTEM WAIT FAILPOINT finish_set_quorum_failed_parts", timeout=300
                )
            )
            first.query(f"SYSTEM START FETCHES {table_name}")

            concurrent.futures.wait([quorum_fail_future])

            assert quorum_fail_future.exception() is None

            zero.query("SYSTEM ENABLE FAILPOINT finish_clean_quorum_failed_parts")
            clean_quorum_fail_parts_future = executor.submit(
                lambda: zero.query(
                    "SYSTEM WAIT FAILPOINT finish_clean_quorum_failed_parts",
                    timeout=300,
                )
            )
            pm.restore_instance_zk_connections(zero)
            concurrent.futures.wait([clean_quorum_fail_parts_future])

            assert clean_quorum_fail_parts_future.exception() is None

            zero.query(
                "SYSTEM DISABLE FAILPOINT replicated_merge_tree_insert_retry_pause"
            )
            concurrent.futures.wait([insert_future])
            assert insert_future.exception() is not None
            assert not zero.contains_in_log("LOGICAL_ERROR")
            assert zero.contains_in_log(
                "fails to commit and will not retry or clean garbage"
            )


def test_insert_quorum_with_keeper_fail_during_unknown_status(started_cluster):
    # Regression for a logical error ("Part ... doesn't exist") / server abort:
    # a quorum insert commits the part to keeper but the client gets a hardware error,
    # and while it is recovering it cannot verify the part status in keeper (eternal
    # hardware error). Meanwhile another replica marks the quorum as failed and the
    # restarting thread's removeFailedQuorumParts() discards the still-PreActive part.
    # When the recovery then gives up and tries to commit the (now gone) part, it used
    # to raise a logical error instead of reporting UNKNOWN_STATUS_OF_INSERT.
    table_name = "test_insert_quorum_keeper_fail_unknown_" + uuid.uuid4().hex
    create_query = (
        f"CREATE TABLE {table_name} "
        "(a Int8, d Date) "
        "Engine = ReplicatedMergeTree('/clickhouse/tables/{table}', '{replica}') "
        "ORDER BY a "
    )

    zero.query(create_query)
    first.query(create_query)

    first.query(f"SYSTEM STOP FETCHES {table_name}")

    zero.query("SYSTEM ENABLE FAILPOINT replicated_merge_tree_commit_zk_fail_after_op")
    zero.query("SYSTEM ENABLE FAILPOINT replicated_merge_tree_insert_retry_pause")
    # Make the recovery unable to verify the part status in keeper, so it gives up and
    # reaches the "unknown status" path that commits the local part.
    zero.query(
        "SYSTEM ENABLE FAILPOINT replicated_merge_tree_commit_zk_fail_when_recovering_from_hw_fault"
    )

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            insert_future = executor.submit(
                lambda: zero.query(
                    f"INSERT INTO {table_name}(a,d) VALUES(1, '2011-01-01')",
                    settings={
                        "insert_quorum_timeout": 150000,
                        "insert_keeper_max_retries": 5,
                        "insert_keeper_retry_max_backoff_ms": 10,
                    },
                )
            )
            try:
                zk = cluster.get_kazoo_client("zoo1")

                # Ensure that part had been committed to keeper
                retries = 0
                while True:
                    if zk.exists(
                        f"/clickhouse/tables/{table_name}/replicas/zero/parts/all_0_0_0"
                    ):
                        break
                    print("replica still did not create all_0_0_0")
                    time.sleep(1)
                    retries += 1
                    if retries == 120:
                        raise Exception("Can not wait for all_0_0_0 part")

                with PartitionManager() as pm:
                    pm.drop_instance_zk_connections(zero)

                    retries = 0
                    while True:
                        if (
                            zk.exists(
                                f"/clickhouse/tables/{table_name}/replicas/zero/is_active"
                            )
                            is None
                        ):
                            break
                        print("replica is still active")
                        time.sleep(1)
                        retries += 1
                        if retries == 120:
                            raise Exception("Can not wait cluster replica inactive")

                    first.query("SYSTEM ENABLE FAILPOINT finish_set_quorum_failed_parts")
                    quorum_fail_future = executor.submit(
                        lambda: first.query(
                            "SYSTEM WAIT FAILPOINT finish_set_quorum_failed_parts",
                            timeout=300,
                        )
                    )
                    first.query(f"SYSTEM START FETCHES {table_name}")

                    concurrent.futures.wait([quorum_fail_future])
                    assert quorum_fail_future.exception() is None

                    # The restarting thread of "zero" discards the failed-quorum part, which is
                    # still PreActive and owned by the recovering insert transaction.
                    zero.query("SYSTEM ENABLE FAILPOINT finish_clean_quorum_failed_parts")
                    clean_quorum_fail_parts_future = executor.submit(
                        lambda: zero.query(
                            "SYSTEM WAIT FAILPOINT finish_clean_quorum_failed_parts",
                            timeout=300,
                        )
                    )
                    pm.restore_instance_zk_connections(zero)
                    concurrent.futures.wait([clean_quorum_fail_parts_future])
                    assert clean_quorum_fail_parts_future.exception() is None

                    # Let the recovery give up; it must report UNKNOWN_STATUS_OF_INSERT, not abort.
                    zero.query(
                        "SYSTEM DISABLE FAILPOINT replicated_merge_tree_insert_retry_pause"
                    )
                    concurrent.futures.wait([insert_future])
                    insert_exception = insert_future.exception()
                    assert insert_exception is not None
                    # The contract preserved by the fix: the client gets UNKNOWN_STATUS_OF_INSERT, not a
                    # different error (e.g. TABLE_IS_READ_ONLY, a raw Keeper error) and not a logical error.
                    assert "UNKNOWN_STATUS_OF_INSERT" in str(insert_exception), str(
                        insert_exception
                    )
                    assert not zero.contains_in_log("LOGICAL_ERROR")
            finally:
                # Release the paused insert before the ThreadPoolExecutor is joined on block
                # exit. Otherwise, if an assertion above fails while the insert is still paused at
                # `replicated_merge_tree_insert_retry_pause`, leaving the `with` block would call
                # `executor.shutdown(wait=True)`, which blocks forever on the paused insert and turns
                # the failure into a hang instead of a clean failure (the outer `finally` that
                # disables the failpoint only runs after the executor is joined).
                zero.query(
                    "SYSTEM DISABLE FAILPOINT replicated_merge_tree_insert_retry_pause"
                )
    finally:
        zero.query(
            "SYSTEM DISABLE FAILPOINT replicated_merge_tree_commit_zk_fail_when_recovering_from_hw_fault"
        )
        zero.query("SYSTEM DISABLE FAILPOINT replicated_merge_tree_insert_retry_pause")
        zero.query(
            "SYSTEM DISABLE FAILPOINT replicated_merge_tree_commit_zk_fail_after_op"
        )


def test_insert_quorum_with_keeper_fail_during_unknown_status_object_storage(
    started_cluster,
):
    # Same regression as test_insert_quorum_with_keeper_fail_during_unknown_status, but on an
    # object-storage (S3) disk. There the discarded part exercises an extra branch in
    # forcefullyMovePartToDetachedAndRemoveFromMemory: a still-PreActive part owns an
    # uncommitted part storage transaction whose writes and the rename into detached/ are only
    # buffered. The fix commits that transaction before renaming, so the failed-quorum part is
    # materialized as detached/noquorum_<part> instead of leaving the blobs orphaned. On a
    # local disk the storage transaction is a no-op (everything is already on disk), so only an
    # object-storage table covers the materialization.
    table_name = "test_insert_quorum_keeper_fail_unknown_s3_" + uuid.uuid4().hex
    create_query = (
        f"CREATE TABLE {table_name} "
        "(a Int8, d Date) "
        "Engine = ReplicatedMergeTree('/clickhouse/tables/{table}', '{replica}') "
        "ORDER BY a "
        "SETTINGS storage_policy = 's3', min_bytes_for_wide_part = 0 "
    )

    zero.query(create_query)
    first.query(create_query)

    first.query(f"SYSTEM STOP FETCHES {table_name}")

    zero.query("SYSTEM ENABLE FAILPOINT replicated_merge_tree_commit_zk_fail_after_op")
    zero.query("SYSTEM ENABLE FAILPOINT replicated_merge_tree_insert_retry_pause")
    # Make the recovery unable to verify the part status in keeper, so it gives up and
    # reaches the "unknown status" path that commits the local part.
    zero.query(
        "SYSTEM ENABLE FAILPOINT replicated_merge_tree_commit_zk_fail_when_recovering_from_hw_fault"
    )

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            insert_future = executor.submit(
                lambda: zero.query(
                    f"INSERT INTO {table_name}(a,d) VALUES(1, '2011-01-01')",
                    settings={
                        "insert_quorum_timeout": 150000,
                        "insert_keeper_max_retries": 5,
                        "insert_keeper_retry_max_backoff_ms": 10,
                    },
                )
            )
            try:
                zk = cluster.get_kazoo_client("zoo1")

                # Ensure that part had been committed to keeper
                retries = 0
                while True:
                    if zk.exists(
                        f"/clickhouse/tables/{table_name}/replicas/zero/parts/all_0_0_0"
                    ):
                        break
                    print("replica still did not create all_0_0_0")
                    time.sleep(1)
                    retries += 1
                    if retries == 120:
                        raise Exception("Can not wait for all_0_0_0 part")

                with PartitionManager() as pm:
                    pm.drop_instance_zk_connections(zero)

                    retries = 0
                    while True:
                        if (
                            zk.exists(
                                f"/clickhouse/tables/{table_name}/replicas/zero/is_active"
                            )
                            is None
                        ):
                            break
                        print("replica is still active")
                        time.sleep(1)
                        retries += 1
                        if retries == 120:
                            raise Exception("Can not wait cluster replica inactive")

                    first.query("SYSTEM ENABLE FAILPOINT finish_set_quorum_failed_parts")
                    quorum_fail_future = executor.submit(
                        lambda: first.query(
                            "SYSTEM WAIT FAILPOINT finish_set_quorum_failed_parts",
                            timeout=300,
                        )
                    )
                    first.query(f"SYSTEM START FETCHES {table_name}")

                    concurrent.futures.wait([quorum_fail_future])
                    assert quorum_fail_future.exception() is None

                    # The restarting thread of "zero" discards the failed-quorum part, which is
                    # still PreActive and owned by the recovering insert transaction.
                    zero.query("SYSTEM ENABLE FAILPOINT finish_clean_quorum_failed_parts")
                    clean_quorum_fail_parts_future = executor.submit(
                        lambda: zero.query(
                            "SYSTEM WAIT FAILPOINT finish_clean_quorum_failed_parts",
                            timeout=300,
                        )
                    )
                    pm.restore_instance_zk_connections(zero)
                    concurrent.futures.wait([clean_quorum_fail_parts_future])
                    assert clean_quorum_fail_parts_future.exception() is None

                    # Let the recovery give up; it must report UNKNOWN_STATUS_OF_INSERT, not abort.
                    zero.query(
                        "SYSTEM DISABLE FAILPOINT replicated_merge_tree_insert_retry_pause"
                    )
                    concurrent.futures.wait([insert_future])
                    insert_exception = insert_future.exception()
                    assert insert_exception is not None
                    assert "UNKNOWN_STATUS_OF_INSERT" in str(insert_exception), str(
                        insert_exception
                    )
                    assert not zero.contains_in_log("LOGICAL_ERROR")

                    # The object-storage part storage transaction was committed when the part was
                    # discarded, so the failed-quorum part is materialized as a detached part on the
                    # s3 disk (reason "noquorum"). Without the fix the buffered rename would be lost
                    # together with the discarded transaction, so this row would be absent and the
                    # written blobs would be orphaned.
                    detached = zero.query(
                        "SELECT name, disk, reason FROM system.detached_parts "
                        f"WHERE table = '{table_name}' AND reason = 'noquorum'"
                    ).strip()
                    assert detached != "", (
                        "failed-quorum part was not materialized as a detached part "
                        "on object storage"
                    )
                    assert "all_0_0_0" in detached, detached
                    assert "s3" in detached, detached
            finally:
                # Release the paused insert before the ThreadPoolExecutor is joined on block
                # exit (see test_insert_quorum_with_keeper_fail_during_unknown_status for why).
                zero.query(
                    "SYSTEM DISABLE FAILPOINT replicated_merge_tree_insert_retry_pause"
                )
    finally:
        zero.query(
            "SYSTEM DISABLE FAILPOINT replicated_merge_tree_commit_zk_fail_when_recovering_from_hw_fault"
        )
        zero.query("SYSTEM DISABLE FAILPOINT replicated_merge_tree_insert_retry_pause")
        zero.query(
            "SYSTEM DISABLE FAILPOINT replicated_merge_tree_commit_zk_fail_after_op"
        )
