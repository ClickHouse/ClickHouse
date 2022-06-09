import pytest

from helpers.cluster import ClickHouseCluster
from multiprocessing.dummy import Pool
from helpers.corrupt_part_data_on_disk import corrupt_part_data_on_disk
from helpers.corrupt_part_data_on_disk import break_part
import time

cluster = ClickHouseCluster(__file__, zookeeper_config_path="configs/zookeeper.xml")

node1 = cluster.add_instance(
    "node1",
    stay_alive=True,
    with_zookeeper=True,
    main_configs=["configs/zookeeper.xml"],
)

path_to_data = "/var/lib/clickhouse/"


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def remove_broken_detached_part_impl(table, node, expect_broken_prefix):
    assert (
        node.query(
            f"SELECT COUNT() FROM system.parts WHERE table='{table}' AND active=1"
        )
        == "4\n"
    )

    path_to_detached = path_to_data + f"data/default/{table}/detached/"

    result = node.exec_in_container(["ls", f"{path_to_detached}"])
    assert result.strip() == ""

    corrupt_part_data_on_disk(node, table, "all_3_3_0")
    break_part(node, table, "all_3_3_0")

    result = node.query(
        f"CHECK TABLE {table}", settings={"check_query_single_value_result": 0}
    )
    assert "all_3_3_0\t0" in result

    node.query(f"DETACH TABLE {table}")
    node.query(f"ATTACH TABLE {table}")

    result = node.exec_in_container(["ls", f"{path_to_detached}"])
    print(result)
    assert f"{expect_broken_prefix}_all_3_3_0" in result

    time.sleep(15)

    result = node.exec_in_container(["ls", f"{path_to_detached}"])
    print(result)
    assert f"{expect_broken_prefix}_all_3_3_0" not in result

    node.query(f"DROP TABLE {table} SYNC")


def test_remove_broken_detached_part_merge_tree(started_cluster):
    node1.query(
        """
        CREATE TABLE mt(id UInt32, value Int32)
        ENGINE = MergeTree() ORDER BY id
        SETTINGS merge_tree_clear_old_broken_detached_parts_interval_seconds=5;
        """
    )

    for i in range(4):
        node1.query(
            f"INSERT INTO mt SELECT number, number * number FROM numbers ({i * 100000}, 100000)"
        )

    remove_broken_detached_part_impl("mt", node1, "broken-on-start")


def test_remove_broken_detached_part_replicated_merge_tree(started_cluster):
    node1.query(
        f"""
        CREATE TABLE replicated_mt(date Date, id UInt32, value Int32)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/replicated_mt', '{node1.name}') ORDER BY id
        SETTINGS merge_tree_clear_old_broken_detached_parts_interval_seconds=5, cleanup_delay_period=1, cleanup_delay_period_random_add=0;
        """
    )

    for i in range(4):
        node1.query(
            f"INSERT INTO replicated_mt SELECT toDate('2019-10-01'), number, number * number FROM numbers ({i * 100000}, 100000)"
        )

    remove_broken_detached_part_impl("replicated_mt", node1, "broken")
