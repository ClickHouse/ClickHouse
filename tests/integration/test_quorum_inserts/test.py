import concurrent
import time

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.network import PartitionManager
from helpers.test_tools import TSV

cluster = ClickHouseCluster(__file__)

zero = cluster.add_instance(
    "zero",
    user_configs=["configs/users.d/settings.xml"],
    main_configs=["configs/config.d/remote_servers.xml"],
    macros={"cluster": "anime", "shard": "0", "replica": "zero"},
    with_zookeeper=True,
)

first = cluster.add_instance(
    "first",
    user_configs=["configs/users.d/settings.xml"],
    main_configs=["configs/config.d/remote_servers.xml"],
    macros={"cluster": "anime", "shard": "0", "replica": "first"},
    with_zookeeper=True,
)

second = cluster.add_instance(
    "second",
    user_configs=["configs/users.d/settings.xml"],
    main_configs=["configs/config.d/remote_servers.xml"],
    macros={"cluster": "anime", "shard": "0", "replica": "second"},
    with_zookeeper=True,
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
    zero.query("DROP TABLE IF EXISTS test_simple ON CLUSTER cluster")

    create_query = (
        "CREATE TABLE test_simple "
        "(a Int8, d Date) "
        "Engine = ReplicatedMergeTree('/clickhouse/tables/{shard}/{table}', '{replica}') "
        "PARTITION BY d ORDER BY a"
    )

    zero.query(create_query)
    first.query(create_query)

    first.query("SYSTEM STOP FETCHES test_simple")

    zero.query(
        "INSERT INTO test_simple VALUES (1, '2011-01-01')",
        settings={"insert_quorum": 1},
    )

    assert "1\t2011-01-01\n" == zero.query("SELECT * from test_simple")
    assert "" == first.query("SELECT * from test_simple")

    first.query("SYSTEM START FETCHES test_simple")

    first.query("SYSTEM SYNC REPLICA test_simple", timeout=20)

    assert "1\t2011-01-01\n" == zero.query("SELECT * from test_simple")
    assert "1\t2011-01-01\n" == first.query("SELECT * from test_simple")

    second.query(create_query)
    second.query("SYSTEM SYNC REPLICA test_simple", timeout=20)

    assert "1\t2011-01-01\n" == zero.query("SELECT * from test_simple")
    assert "1\t2011-01-01\n" == first.query("SELECT * from test_simple")
    assert "1\t2011-01-01\n" == second.query("SELECT * from test_simple")

    zero.query("DROP TABLE IF EXISTS test_simple ON CLUSTER cluster")


def test_drop_replica_and_achieve_quorum(started_cluster):
    zero.query(
        "DROP TABLE IF EXISTS test_drop_replica_and_achieve_quorum ON CLUSTER cluster"
    )

    create_query = (
        "CREATE TABLE test_drop_replica_and_achieve_quorum "
        "(a Int8, d Date) "
        "Engine = ReplicatedMergeTree('/clickhouse/tables/{shard}/{table}', '{replica}') "
        "PARTITION BY d ORDER BY a"
    )

    print("Create Replicated table with two replicas")
    zero.query(create_query)
    first.query(create_query)

    print("Stop fetches on one replica. Since that, it will be isolated.")
    first.query("SYSTEM STOP FETCHES test_drop_replica_and_achieve_quorum")

    print("Insert to other replica. This query will fail.")
    quorum_timeout = zero.query_and_get_error(
        "INSERT INTO test_drop_replica_and_achieve_quorum(a,d) VALUES (1, '2011-01-01')",
        settings={"insert_quorum_timeout": 5000},
    )
    assert "Timeout while waiting for quorum" in quorum_timeout, "Query must fail."

    assert TSV("1\t2011-01-01\n") == TSV(
        zero.query(
            "SELECT * FROM test_drop_replica_and_achieve_quorum",
            settings={"select_sequential_consistency": 0},
        )
    )

    assert TSV("") == TSV(
        zero.query(
            "SELECT * FROM test_drop_replica_and_achieve_quorum",
            settings={"select_sequential_consistency": 1},
        )
    )

    # TODO:(Mikhaylov) begin; maybe delete this lines. I want clickhouse to fetch parts and update quorum.
    print("START FETCHES first replica")
    first.query("SYSTEM START FETCHES test_drop_replica_and_achieve_quorum")

    print("SYNC first replica")
    first.query("SYSTEM SYNC REPLICA test_drop_replica_and_achieve_quorum", timeout=20)
    # TODO:(Mikhaylov) end

    print("Add second replica")
    second.query(create_query)

    print("SYNC second replica")
    second.query("SYSTEM SYNC REPLICA test_drop_replica_and_achieve_quorum", timeout=20)

    print("Quorum for previous insert achieved.")
    assert TSV("1\t2011-01-01\n") == TSV(
        second.query(
            "SELECT * FROM test_drop_replica_and_achieve_quorum",
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
    )
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
        assert TSV("") == TSV(zero.query(f"SELECT * FROM {table_name}"))
        assert TSV("") == TSV(second.query(f"SELECT * FROM {table_name}"))

    zero.query(f"DROP TABLE IF EXISTS {table_name} ON CLUSTER cluster")


@pytest.mark.parametrize(("add_new_data"), [False, True])
def test_insert_quorum_with_move_partition(started_cluster, add_new_data):
    # use different table names for easier disambiguation in logs between runs (you may also check uuid though, but not always convenient)
    source_table_name = (
        "test_insert_quorum_with_move_partition_source_new_data"
        if add_new_data
        else "test_insert_quorum_with_move_partition_source"
    )
    destination_table_name = (
        "test_insert_quorum_with_move_partition_destination_new_data"
        if add_new_data
        else "test_insert_quorum_with_move_partition_destination"
    )
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
        assert TSV("") == TSV(zero.query(f"SELECT * FROM {source_table_name}"))
        assert TSV("") == TSV(second.query(f"SELECT * FROM {source_table_name}"))

    zero.query(f"DROP TABLE IF EXISTS {source_table_name} ON CLUSTER cluster")
    zero.query(f"DROP TABLE IF EXISTS {destination_table_name} ON CLUSTER cluster")


def test_insert_quorum_with_ttl(started_cluster):
    zero.query("DROP TABLE IF EXISTS test_insert_quorum_with_ttl ON CLUSTER cluster")

    create_query = (
        "CREATE TABLE test_insert_quorum_with_ttl "
        "(a Int8, d Date) "
        "Engine = ReplicatedMergeTree('/clickhouse/tables/{table}', '{replica}') "
        "PARTITION BY d ORDER BY a "
        "TTL d + INTERVAL 5 second DELETE WHERE toYear(d) = 2011 "
        "SETTINGS merge_with_ttl_timeout=2 "
    )

    print("Create Replicated table with two replicas")
    zero.query(create_query)
    first.query(create_query)

    print("Stop fetches for test_insert_quorum_with_ttl at first replica.")
    first.query("SYSTEM STOP FETCHES test_insert_quorum_with_ttl")

    print("Insert should fail since it can not reach the quorum.")
    quorum_timeout = zero.query_and_get_error(
        "INSERT INTO test_insert_quorum_with_ttl(a,d) VALUES(1, '2011-01-01')",
        settings={"insert_quorum_timeout": 5000},
    )
    assert "Timeout while waiting for quorum" in quorum_timeout, "Query must fail."

    print(
        "Wait 10 seconds and TTL merge have to be executed. But it won't delete data."
    )
    time.sleep(10)
    assert TSV("1\t2011-01-01\n") == TSV(
        zero.query(
            "SELECT * FROM test_insert_quorum_with_ttl",
            settings={"select_sequential_consistency": 0},
        )
    )

    print("Resume fetches for test_insert_quorum_with_ttl at first replica.")
    first.query("SYSTEM START FETCHES test_insert_quorum_with_ttl")

    print("Sync first replica.")
    first.query("SYSTEM SYNC REPLICA test_insert_quorum_with_ttl")

    zero.query(
        "INSERT INTO test_insert_quorum_with_ttl(a,d) VALUES(1, '2011-01-01')",
        settings={"insert_quorum_timeout": 5000},
    )

    print("Inserts should resume.")
    zero.query("INSERT INTO test_insert_quorum_with_ttl(a, d) VALUES(2, '2012-02-02')")

    first.query("OPTIMIZE TABLE test_insert_quorum_with_ttl")
    first.query("SYSTEM SYNC REPLICA test_insert_quorum_with_ttl")
    zero.query("SYSTEM SYNC REPLICA test_insert_quorum_with_ttl")

    assert TSV("2\t2012-02-02\n") == TSV(
        first.query(
            "SELECT * FROM test_insert_quorum_with_ttl",
            settings={"select_sequential_consistency": 0},
        )
    )
    assert TSV("2\t2012-02-02\n") == TSV(
        first.query(
            "SELECT * FROM test_insert_quorum_with_ttl",
            settings={"select_sequential_consistency": 1},
        )
    )

    zero.query("DROP TABLE IF EXISTS test_insert_quorum_with_ttl ON CLUSTER cluster")


def test_insert_quorum_with_keeper_loss_connection():
    zero.query(
        "DROP TABLE IF EXISTS test_insert_quorum_with_keeper_fail ON CLUSTER cluster"
    )
    create_query = (
        "CREATE TABLE test_insert_quorum_with_keeper_loss"
        "(a Int8, d Date) "
        "Engine = ReplicatedMergeTree('/clickhouse/tables/{table}', '{replica}') "
        "ORDER BY a "
    )

    zero.query(create_query)
    first.query(create_query)

    first.query("SYSTEM STOP FETCHES test_insert_quorum_with_keeper_loss")

    zero.query("SYSTEM ENABLE FAILPOINT replicated_merge_tree_commit_zk_fail_after_op")
    zero.query("SYSTEM ENABLE FAILPOINT replicated_merge_tree_insert_retry_pause")

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        insert_future = executor.submit(
            lambda: zero.query(
                "INSERT INTO test_insert_quorum_with_keeper_loss(a,d) VALUES(1, '2011-01-01')",
                settings={"insert_quorum_timeout": 150000},
            )
        )

        pm = PartitionManager()
        pm.drop_instance_zk_connections(zero)

        retries = 0
        zk = cluster.get_kazoo_client("zoo1")
        while True:
            if (
                zk.exists(
                    "/clickhouse/tables/test_insert_quorum_with_keeper_loss/replicas/zero/is_active"
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
        first.query("SYSTEM START FETCHES test_insert_quorum_with_keeper_loss")

        concurrent.futures.wait([quorum_fail_future])

        assert quorum_fail_future.exception() is None

        zero.query("SYSTEM ENABLE FAILPOINT finish_clean_quorum_failed_parts")
        clean_quorum_fail_parts_future = executor.submit(
            lambda: first.query(
                "SYSTEM WAIT FAILPOINT finish_clean_quorum_failed_parts", timeout=300
            )
        )
        pm.restore_instance_zk_connections(zero)
        concurrent.futures.wait([clean_quorum_fail_parts_future])

        assert clean_quorum_fail_parts_future.exception() is None

        zero.query("SYSTEM DISABLE FAILPOINT replicated_merge_tree_insert_retry_pause")
        concurrent.futures.wait([insert_future])
        assert insert_future.exception() is not None
        assert not zero.contains_in_log("LOGICAL_ERROR")
        assert zero.contains_in_log(
            "fails to commit and will not retry or clean garbage"
        )
