import threading
import time

import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance('node1',
                             main_configs=['configs/logs_config.xml'],
                             with_zookeeper=True,
                             macros={"shard": 0, "replica": 1})

node2 = cluster.add_instance('node2',
                             main_configs=['configs/logs_config.xml'],
                             with_zookeeper=True,
                             macros={"shard": 0, "replica": 2})


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        node1.query('CREATE DATABASE test ENGINE=Ordinary') # Different paths with Atomic
        node2.query('CREATE DATABASE test ENGINE=Ordinary')
        yield cluster

    finally:
        cluster.shutdown()


def split_tsv(data):
    return [x.split("\t") for x in data.splitlines()]


@pytest.mark.parametrize("replicated", [
    "",
    "replicated"
])
def test_merge_simple(started_cluster, replicated):
    try:
        clickhouse_path = "/var/lib/clickhouse"
        db_name = "test"
        table_name = "merge_simple"
        name = db_name + "." + table_name
        table_path = "data/" + db_name + "/" + table_name
        nodes = [node1, node2] if replicated else [node1]
        engine = "ReplicatedMergeTree('/clickhouse/test_merge_simple', '{replica}')" if replicated else "MergeTree()"
        node_check = nodes[-1]
        starting_block = 0 if replicated else 1

        for node in nodes:
            node.query("""
                CREATE TABLE {name}
                (
                    `a` Int64
                )
                ENGINE = {engine}
                ORDER BY sleep(2)
            """.format(engine=engine, name=name))

        node1.query("INSERT INTO {name} VALUES (1)".format(name=name))
        node1.query("INSERT INTO {name} VALUES (2)".format(name=name))
        node1.query("INSERT INTO {name} VALUES (3)".format(name=name))

        parts = ["all_{}_{}_0".format(x, x) for x in range(starting_block, starting_block + 3)]
        result_part = "all_{}_{}_1".format(starting_block, starting_block + 2)

        def optimize():
            node1.query("OPTIMIZE TABLE {name}".format(name=name))

        wait = threading.Thread(target=time.sleep, args=(5,))
        wait.start()
        t = threading.Thread(target=optimize)
        t.start()

        time.sleep(1)
        assert split_tsv(node_check.query("""
            SELECT database, table, num_parts, source_part_names, source_part_paths, result_part_name, result_part_path, partition_id, is_mutation
                FROM system.merges
                WHERE table = '{name}'
        """.format(name=table_name))) == [
            [
                db_name,
                table_name,
                "3",
                "['{}','{}','{}']".format(*parts),
                "['{clickhouse}/{table_path}/{}/','{clickhouse}/{table_path}/{}/','{clickhouse}/{table_path}/{}/']".format(
                    *parts, clickhouse=clickhouse_path, table_path=table_path),
                result_part,
                "{clickhouse}/{table_path}/{}/".format(result_part, clickhouse=clickhouse_path, table_path=table_path),
                "all",
                "0"
            ]
        ]
        t.join()
        wait.join()

        assert node_check.query("SELECT * FROM system.merges WHERE table = '{name}'".format(name=table_name)) == ""

    finally:
        for node in nodes:
            node.query("DROP TABLE {name}".format(name=name))


@pytest.mark.parametrize("replicated", [
    "",
    "replicated"
])
def test_mutation_simple(started_cluster, replicated):
    try:
        clickhouse_path = "/var/lib/clickhouse"
        db_name = "test"
        table_name = "mutation_simple"
        name = db_name + "." + table_name
        table_path = "data/" + db_name + "/" + table_name
        nodes = [node1, node2] if replicated else [node1]
        engine = "ReplicatedMergeTree('/clickhouse/test_mutation_simple', '{replica}')" if replicated else "MergeTree()"
        node_check = nodes[-1]
        starting_block = 0 if replicated else 1

        for node in nodes:
            node.query("""
                CREATE TABLE {name}
                (
                    `a` Int64
                )
                ENGINE = {engine}
                ORDER BY tuple()
            """.format(engine=engine, name=name))

        node1.query("INSERT INTO {name} VALUES (1)".format(name=name))
        part = "all_{}_{}_0".format(starting_block, starting_block)
        result_part = "all_{}_{}_0_{}".format(starting_block, starting_block, starting_block + 1)

        def alter():
            node1.query("ALTER TABLE {name} UPDATE a = 42 WHERE sleep(2) OR 1".format(name=name))

        t = threading.Thread(target=alter)
        t.start()

        time.sleep(1)
        assert split_tsv(node_check.query("""
            SELECT database, table, num_parts, source_part_names, source_part_paths, result_part_name, result_part_path, partition_id, is_mutation
                FROM system.merges
                WHERE table = '{name}'
        """.format(name=table_name))) == [
            [
                db_name,
                table_name,
                "1",
                "['{}']".format(part),
                "['{clickhouse}/{table_path}/{}/']".format(part, clickhouse=clickhouse_path, table_path=table_path),
                result_part,
                "{clickhouse}/{table_path}/{}/".format(result_part, clickhouse=clickhouse_path, table_path=table_path),
                "all",
                "1"
            ],
        ]
        t.join()

        time.sleep(1.5)

        assert node_check.query("SELECT * FROM system.merges WHERE table = '{name}'".format(name=table_name)) == ""

    finally:
        for node in nodes:
            node.query("DROP TABLE {name}".format(name=name))
