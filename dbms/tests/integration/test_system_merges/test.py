import pytest
import threading
import time
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance('node1',
            config_dir='configs',
            main_configs=['configs/logs_config.xml'],
            with_zookeeper=True,
            macros={"shard": 0, "replica": 1} )

node2 = cluster.add_instance('node2',
            config_dir='configs',
            main_configs=['configs/logs_config.xml'],
            with_zookeeper=True,
            macros={"shard": 0, "replica": 2} )


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def split_tsv(data):
    return [ x.split("\t") for x in data.splitlines() ]


def test_merge_simple(started_cluster):
    try:
        clickhouse_path = "/var/lib/clickhouse"
        name = "test_merge_simple"

        node1.query("""
            CREATE TABLE {name}
            (
                `a` Int64
            )
            ENGINE = MergeTree()
            ORDER BY sleep(2)
        """.format(name=name))
        node1.query("INSERT INTO {name} VALUES (1)".format(name=name))
        node1.query("INSERT INTO {name} VALUES (2)".format(name=name))
        node1.query("INSERT INTO {name} VALUES (3)".format(name=name))

        def optimize():
            node1.query("OPTIMIZE TABLE {name}".format(name=name))

        t = threading.Thread(target=optimize)
        t.start()

        time.sleep(1)
        assert split_tsv(node1.query("""
            SELECT database, table, num_parts, source_part_names, source_part_paths, result_part_name, result_part_path, partition_id, is_mutation
                FROM system.merges
                WHERE table = '{name}'
        """.format(name=name))) == [
            [
                "default",
                name,
                "3",
                "['all_1_1_0','all_2_2_0','all_3_3_0']",
                "['{clickhouse}/data/default/{name}/all_1_1_0/','{clickhouse}/data/default/{name}/all_2_2_0/','{clickhouse}/data/default/{name}/all_3_3_0/']".format(clickhouse=clickhouse_path, name=name),
                "all_1_3_1",
                "{clickhouse}/data/default/{name}/all_1_3_1/".format(clickhouse=clickhouse_path, name=name),
                "all",
                "0"
            ]
        ]
        t.join()

        assert node1.query("SELECT * FROM system.merges WHERE table = '{name}'".format(name=name)) == ""

    finally:
        node1.query("DROP TABLE {name}".format(name=name))


def test_mutation_simple(started_cluster):
    try:
        clickhouse_path = "/var/lib/clickhouse"
        name = "test_mutation_simple"

        node1.query("""
            CREATE TABLE {name}
            (
                `a` Int64
            )
            ENGINE = MergeTree()
            ORDER BY tuple()
        """.format(name=name))
        node1.query("INSERT INTO {name} VALUES (1)".format(name=name))

        def alter():
            node1.query("ALTER TABLE {name} UPDATE a = 42 WHERE sleep(2) OR 1".format(name=name))

        t = threading.Thread(target=alter)
        t.start()

        time.sleep(1)
        assert split_tsv(node1.query("""
            SELECT database, table, num_parts, source_part_names, source_part_paths, result_part_name, result_part_path, partition_id, is_mutation
                FROM system.merges
                WHERE table = '{name}'
        """.format(name=name))) == [
            [
                "default",
                name,
                "1",
                "['all_1_1_0']",
                "['/var/lib/clickhouse/data/default/test_mutation_simple/all_1_1_0/']",
                "all_1_1_0_2",
                "/var/lib/clickhouse/data/default/test_mutation_simple/all_1_1_0_2/",
                "all",
                "1"
            ],
        ]
        t.join()

        time.sleep(1.5)

        assert node1.query("SELECT * FROM system.merges WHERE table = '{name}'".format(name=name)) == ""

    finally:
        node1.query("DROP TABLE {name}".format(name=name))

#def test_replicated(started_cluster):

