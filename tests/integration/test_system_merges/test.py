import threading
import time

import pytest
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/logs_config.xml"],
    user_configs=["configs/user_overrides.xml"],
    with_zookeeper=True,
    macros={"shard": 0, "replica": 1},
)

node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/logs_config.xml"],
    user_configs=["configs/user_overrides.xml"],
    with_zookeeper=True,
    macros={"shard": 0, "replica": 2},
)

settings = {
    "mutations_sync": 2,
    "replication_alter_partitions_sync": 2,
    "optimize_throw_if_noop": 1,
}


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        node1.query(
            "CREATE DATABASE test ENGINE=Ordinary",
            settings={"allow_deprecated_database_ordinary": 1},
        )  # Different paths with Atomic
        node2.query(
            "CREATE DATABASE test ENGINE=Ordinary",
            settings={"allow_deprecated_database_ordinary": 1},
        )
        yield cluster

    finally:
        cluster.shutdown()


def split_tsv(data):
    return [x.split("\t") for x in data.splitlines()]


@pytest.mark.parametrize("replicated", ["", "replicated"])
def test_merge_simple(started_cluster, replicated):
    clickhouse_path = "/var/lib/clickhouse"
    db_name = "test"
    table_name = "merge_simple"
    name = db_name + "." + table_name
    table_path = "data/" + db_name + "/" + table_name
    nodes = [node1, node2] if replicated else [node1]
    engine = (
        "ReplicatedMergeTree('/clickhouse/test_merge_simple', '{replica}')"
        if replicated
        else "MergeTree()"
    )
    node_check = nodes[-1]
    starting_block = 0 if replicated else 1

    try:
        for node in nodes:
            node.query(
                f"create table {name} (a Int64) engine={engine} order by tuple()"
            )

        node1.query(f"INSERT INTO {name} VALUES (1)")
        node1.query(f"INSERT INTO {name} VALUES (2)")
        node1.query(f"INSERT INTO {name} VALUES (3)")

        node1.query(
            f"alter table {name} add column b int materialized sleepEachRow(3)",
            settings=settings,
        )

        parts = [
            "all_{}_{}_0".format(x, x)
            for x in range(starting_block, starting_block + 3)
        ]
        result_part = "all_{}_{}_1".format(starting_block, starting_block + 2)

        # OPTIMIZE will sleep for 3s * 3 (parts) = 9s
        def optimize():
            node1.query("OPTIMIZE TABLE {name}".format(name=name), settings=settings)

        t = threading.Thread(target=optimize)
        t.start()

        # Wait for OPTIMIZE to actually start
        assert_eq_with_retry(
            node_check,
            f"select count() from system.merges where table='{table_name}'",
            "1\n",
            retry_count=30,
            sleep_time=0.1,
        )

        assert (
            split_tsv(
                node_check.query(
                    """
            SELECT database, table, num_parts, source_part_names, source_part_paths, result_part_name, result_part_path, partition_id, is_mutation
                FROM system.merges
                WHERE table = '{name}'
        """.format(
                        name=table_name
                    )
                )
            )
            == [
                [
                    db_name,
                    table_name,
                    "3",
                    "['{}','{}','{}']".format(*parts),
                    "['{clickhouse}/{table_path}/{}/','{clickhouse}/{table_path}/{}/','{clickhouse}/{table_path}/{}/']".format(
                        *parts, clickhouse=clickhouse_path, table_path=table_path
                    ),
                    result_part,
                    "{clickhouse}/{table_path}/{}/".format(
                        result_part, clickhouse=clickhouse_path, table_path=table_path
                    ),
                    "all",
                    "0",
                ]
            ]
        )
        t.join()

        # It still can show a row with progress=1, because OPTIMIZE returns before the entry is removed from MergeList
        assert (
            node_check.query(
                f"SELECT * FROM system.merges WHERE table = '{table_name}' and progress < 1"
            )
            == ""
        )

        # It will eventually disappear
        assert_eq_with_retry(
            node_check,
            f"SELECT * FROM system.merges WHERE table = '{table_name}' and progress < 1",
            "\n",
        )
    finally:
        for node in nodes:
            node.query("DROP TABLE {name}".format(name=name))


@pytest.mark.parametrize("replicated", ["", "replicated"])
def test_mutation_simple(started_cluster, replicated):
    clickhouse_path = "/var/lib/clickhouse"
    db_name = "test"
    table_name = "mutation_simple"
    name = db_name + "." + table_name
    table_path = "data/" + db_name + "/" + table_name
    nodes = [node1, node2] if replicated else [node1]
    engine = (
        "ReplicatedMergeTree('/clickhouse/test_mutation_simple', '{replica}')"
        if replicated
        else "MergeTree()"
    )
    node_check = nodes[-1]
    starting_block = 0 if replicated else 1

    try:
        for node in nodes:
            node.query(
                f"create table {name} (a Int64) engine={engine} order by tuple()"
            )

        node1.query(f"INSERT INTO {name} VALUES (1), (2), (3)")

        part = "all_{}_{}_0".format(starting_block, starting_block)
        result_part = "all_{}_{}_0_{}".format(
            starting_block, starting_block, starting_block + 1
        )

        # ALTER will sleep for 9s
        def alter():
            node1.query(
                f"ALTER TABLE {name} UPDATE a = 42 WHERE sleep(9) OR 1",
                settings=settings,
            )

        t = threading.Thread(target=alter)
        t.start()

        # Wait for the mutation to actually start
        assert_eq_with_retry(
            node_check,
            f"select count() from system.merges where table='{table_name}'",
            "1\n",
            retry_count=30,
            sleep_time=0.1,
        )

        assert (
            split_tsv(
                node_check.query(
                    """
            SELECT database, table, num_parts, source_part_names, source_part_paths, result_part_name, result_part_path, partition_id, is_mutation
                FROM system.merges
                WHERE table = '{name}'
        """.format(
                        name=table_name
                    )
                )
            )
            == [
                [
                    db_name,
                    table_name,
                    "1",
                    "['{}']".format(part),
                    "['{clickhouse}/{table_path}/{}/']".format(
                        part, clickhouse=clickhouse_path, table_path=table_path
                    ),
                    result_part,
                    "{clickhouse}/{table_path}/{}/".format(
                        result_part, clickhouse=clickhouse_path, table_path=table_path
                    ),
                    "all",
                    "1",
                ],
            ]
        )
        t.join()

        assert (
            node_check.query(
                f"SELECT * FROM system.merges WHERE table = '{table_name}' and progress < 1"
            )
            == ""
        )

        # It will eventually disappear
        assert_eq_with_retry(
            node_check,
            f"SELECT * FROM system.merges WHERE table = '{table_name}' and progress < 1",
            "\n",
        )

    finally:
        for node in nodes:
            node.query("DROP TABLE {name}".format(name=name))
