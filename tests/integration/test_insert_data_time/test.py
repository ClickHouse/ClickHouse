import logging
import time

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.config_manager import ConfigManager

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    with_zookeeper=True,
    main_configs=[
        "configs/cluster.xml",
        "configs/min_max_data_insert_time.xml",
        "configs/macro.xml",
    ],
    macros={"replica": "node1"},
)
node2 = cluster.add_instance(
    "node2",
    with_zookeeper=True,
    main_configs=[
        "configs/cluster.xml",
        "configs/min_max_data_insert_time.xml",
        "configs/macro.xml",
    ],
    macros={"replica": "node2"},
)

node_old = cluster.add_instance(
    "node_with_old_ch",
    image="clickhouse/clickhouse-server",
    tag="24.3",
    with_installed_binary=True,
    stay_alive=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster

    finally:
        cluster.shutdown()


def get_max_min_time_of_data_insert(node, db_name, table_name):
    return (
        node.query(
            f"SELECT min(min_time_of_data_insert), max(max_time_of_data_insert) FROM system.parts WHERE database='{db_name}' AND table='{table_name}' AND active=1"
        )
        .strip()
        .split("\t")
    )


def test_merge(started_cluster):
    db_name = "test_db"
    table_name = "test_table"
    node = node1

    node.query(f"DROP DATABASE IF EXISTS {db_name}")
    node.query(f"CREATE DATABASE {db_name}")
    node.query(
        f"CREATE TABLE {db_name}.{table_name} (a int) ENGINE = MergeTree() ORDER BY a"
    )
    node.query(f"INSERT INTO {db_name}.{table_name} SELECT 1")
    time.sleep(1)
    node.query(f"INSERT INTO {db_name}.{table_name} SELECT 2")
    [min_time, max_time] = get_max_min_time_of_data_insert(node, db_name, table_name)

    print(min_time, max_time)
    assert min_time != max_time

    node.query(f"OPTIMIZE TABLE {db_name}.{table_name}")
    [min_time_new, max_time_new] = get_max_min_time_of_data_insert(
        node, db_name, table_name
    )

    assert min_time_new == min_time and max_time_new == max_time


def test_mutations(started_cluster):
    db_name = "test_db"
    table_name = "test_table"
    node = node1

    node.query(f"DROP DATABASE IF EXISTS {db_name}")
    node.query(f"CREATE DATABASE {db_name}")
    node.query(
        f"CREATE TABLE {db_name}.{table_name} (a int, b int) ENGINE = MergeTree() ORDER BY a"
    )
    node.query(f"INSERT INTO {db_name}.{table_name} SELECT 1, 1")
    [min_time, max_time] = get_max_min_time_of_data_insert(node, db_name, table_name)
    print(min_time, max_time)
    assert min_time == max_time

    time.sleep(1)
    node.query(f"ALTER TABLE {db_name}.{table_name} UPDATE b = 2 WHERE b = 1")
    [min_time_new, max_time_new] = get_max_min_time_of_data_insert(
        node, db_name, table_name
    )

    assert min_time == min_time_new and max_time == max_time_new


def test_move_partition(started_cluster):
    db_name = "test_db"
    table_name1 = "test_table1"
    table_name2 = "test_table2"
    node = node1

    node.query(f"DROP DATABASE IF EXISTS {db_name}")
    node.query(f"CREATE DATABASE {db_name}")
    node.query(
        f"CREATE TABLE {db_name}.{table_name1} (a int, b int) ENGINE = MergeTree() ORDER BY a PARTITION BY a"
    )
    node.query(
        f"CREATE TABLE {db_name}.{table_name2} (a int, b int) ENGINE = MergeTree() ORDER BY a PARTITION BY a"
    )
    node.query(f"INSERT INTO {db_name}.{table_name1} SELECT 1, 1")
    [min_time, max_time] = get_max_min_time_of_data_insert(node, db_name, table_name1)

    partition_name = (
        node.query(
            f"SELECT partition FROM system.parts where database='{db_name}' AND table='{table_name1}' AND active=1"
        )
        .strip()
        .split("\t")
    )[0]
    assert min_time == max_time

    time.sleep(1)
    node.query(
        f"ALTER TABLE {db_name}.{table_name1} MOVE PARTITION '{partition_name}' TO TABLE {db_name}.{table_name2}"
    )
    [min_time_new, max_time_new] = get_max_min_time_of_data_insert(
        node, db_name, table_name2
    )

    assert min_time == min_time_new and max_time == max_time_new


def test_replicated_fetch(started_cluster):
    db_name = "test_db"
    table_name = "test_table"

    node1.query(f"DROP DATABASE IF EXISTS {db_name} ON CLUSTER '{{cluster}}' SYNC")
    node1.query(f"CREATE DATABASE {db_name} ON CLUSTER '{{cluster}}'")
    node1.query(
        f"CREATE TABLE {db_name}.{table_name} ON CLUSTER '{{cluster}}' (a int) ENGINE = ReplicatedMergeTree('/clickhouse/tables/test_table/replicated', '{{replica}}') ORDER BY a"
    )
    node1.query(f"INSERT INTO {db_name}.{table_name} SELECT 1")

    [min_time_node1, max_time_node1] = get_max_min_time_of_data_insert(
        node1, db_name, table_name
    )
    [min_time_node2, max_time_node2] = get_max_min_time_of_data_insert(
        node2, db_name, table_name
    )
    assert min_time_node1 == min_time_node2 and max_time_node1 == max_time_node2

    node2.query(f"INSERT INTO {db_name}.{table_name} SELECT 2")
    node2.query(f"OPTIMIZE TABLE {db_name}.{table_name}")

    [min_time_node1, max_time_node1] = get_max_min_time_of_data_insert(
        node1, db_name, table_name
    )
    [min_time_node2, max_time_node2] = get_max_min_time_of_data_insert(
        node2, db_name, table_name
    )
    assert min_time_node1 == min_time_node2 and max_time_node1 == max_time_node2
    node1.query(f"DROP DATABASE IF EXISTS {db_name} ON CLUSTER '{{cluster}}' SYNC")


def test_version_upgrade(started_cluster):
    db_name = "test_db"
    table_name = "test_table"
    node = node_old

    node.query(f"DROP DATABASE IF EXISTS {db_name}")
    node.query(f"CREATE DATABASE {db_name}")
    node.query(
        f"CREATE TABLE {db_name}.{table_name} (a int) ENGINE = MergeTree() ORDER BY a"
    )
    node.query(f"INSERT INTO {db_name}.{table_name} SELECT 1")

    modification_time = (
        node.query(
            f"SELECT modification_time FROM system.parts WHERE database='{db_name}' AND table='{table_name}' AND active=1"
        )
        .strip()
        .split("\t")
    )[0]

    node.restart_with_latest_version()

    with ConfigManager() as config_manager:
        config_manager.add_main_config(
            node, "configs/min_max_data_insert_time.xml", reload_config=True
        )
        node.restart_clickhouse()
        # For old parts modification time will be equal modification time.
        [min_time_node, max_time_node] = get_max_min_time_of_data_insert(
            node, db_name, table_name
        )

        assert min_time_node == modification_time and max_time_node == modification_time

        time.sleep(1)
        node.query(f"INSERT INTO {db_name}.{table_name} SELECT 2")
        [min_time_node, max_time_node] = get_max_min_time_of_data_insert(
            node, db_name, table_name
        )
        assert min_time_node != max_time_node

        node.query(f"DROP DATABASE IF EXISTS {db_name} SYNC")
        config_manager.reset()

    node.restart_with_original_version()
