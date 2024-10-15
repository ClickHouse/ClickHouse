import os

import pytest

from helpers.cluster import ClickHouseCluster

CONFIG_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), "configs")

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/enable_keeper_map.xml"],
    user_configs=["configs/keeper_retries.xml"],
    with_zookeeper=True,
    stay_alive=True,
)


def start_clean_clickhouse():
    # remove fault injection if present
    if "fault_injection.xml" in node.exec_in_container(
        ["bash", "-c", "ls /etc/clickhouse-server/config.d"]
    ):
        print("Removing fault injection")
        node.exec_in_container(
            ["bash", "-c", "rm /etc/clickhouse-server/config.d/fault_injection.xml"]
        )
        node.restart_clickhouse()


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def repeat_query(query, repeat):
    for _ in range(repeat):
        node.query(
            query,
        )


def test_queries(started_cluster):
    start_clean_clickhouse()

    node.query("DROP TABLE IF EXISTS keeper_map_retries SYNC")
    node.stop_clickhouse()
    node.copy_file_to_container(
        os.path.join(CONFIG_DIR, "fault_injection.xml"),
        "/etc/clickhouse-server/config.d/fault_injection.xml",
    )
    node.start_clickhouse()

    repeat_count = 10

    node.query(
        "CREATE TABLE keeper_map_retries (a UInt64, b UInt64) Engine=KeeperMap('/keeper_map_retries') PRIMARY KEY a",
    )

    repeat_query(
        "INSERT INTO keeper_map_retries SELECT number, number FROM numbers(500)",
        repeat_count,
    )
    repeat_query("SELECT * FROM keeper_map_retries", repeat_count)
    repeat_query(
        "ALTER TABLE keeper_map_retries UPDATE b = 3 WHERE a > 2", repeat_count
    )
    repeat_query("ALTER TABLE keeper_map_retries DELETE WHERE a > 2", repeat_count)
    repeat_query("TRUNCATE keeper_map_retries", repeat_count)
