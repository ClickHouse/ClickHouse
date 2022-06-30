import pytest

from helpers.cluster import ClickHouseCluster
from multiprocessing.dummy import Pool
from helpers.corrupt_part_data_on_disk import corrupt_part_data_on_disk
from helpers.corrupt_part_data_on_disk import break_part
import time

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance("node1", stay_alive=True, with_zookeeper=True)

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

    result = node.exec_in_container(["ls", path_to_detached])
    assert result.strip() == ""

    corrupt_part_data_on_disk(node, table, "all_3_3_0")
    break_part(node, table, "all_3_3_0")
    node.query(f"ALTER TABLE {table} DETACH PART 'all_1_1_0'")
    result = node.exec_in_container(["touch", f"{path_to_detached}trash"])

    node.exec_in_container(["mkdir", f"{path_to_detached}../broken_all_fake"])
    node.exec_in_container(
        ["touch", "-t", "1312031429.30", f"{path_to_detached}../broken_all_fake"]
    )
    result = node.exec_in_container(["stat", f"{path_to_detached}../broken_all_fake"])
    print(result)
    assert "Modify: 2013-12-03" in result
    node.exec_in_container(
        [
            "mv",
            f"{path_to_detached}../broken_all_fake",
            f"{path_to_detached}broken_all_fake",
        ]
    )

    node.exec_in_container(["mkdir", f"{path_to_detached}../unexpected_all_42_1337_5"])
    node.exec_in_container(
        [
            "touch",
            "-t",
            "1312031429.30",
            f"{path_to_detached}../unexpected_all_42_1337_5",
        ]
    )
    result = node.exec_in_container(
        ["stat", f"{path_to_detached}../unexpected_all_42_1337_5"]
    )
    print(result)
    assert "Modify: 2013-12-03" in result
    node.exec_in_container(
        [
            "mv",
            f"{path_to_detached}../unexpected_all_42_1337_5",
            f"{path_to_detached}unexpected_all_42_1337_5",
        ]
    )

    result = node.query(
        f"CHECK TABLE {table}", settings={"check_query_single_value_result": 0}
    )
    assert "all_3_3_0\t0" in result

    node.query(f"DETACH TABLE {table}")
    node.query(f"ATTACH TABLE {table}")

    result = node.exec_in_container(["ls", path_to_detached])
    print(result)
    assert f"{expect_broken_prefix}_all_3_3_0" in result
    assert "all_1_1_0" in result
    assert "trash" in result
    assert "broken_all_fake" in result
    assert "unexpected_all_42_1337_5" in result

    time.sleep(15)

    result = node.exec_in_container(["ls", path_to_detached])
    print(result)
    assert f"{expect_broken_prefix}_all_3_3_0" not in result
    assert "all_1_1_0" in result
    assert "trash" in result
    assert "broken_all_fake" in result
    assert "unexpected_all_42_1337_5" not in result

    node.query(f"DROP TABLE {table} SYNC")


def test_remove_broken_detached_part_merge_tree(started_cluster):
    node1.query(
        """
        CREATE TABLE
            mt(id UInt32, value Int32)
        ENGINE = MergeTree() ORDER BY id
        SETTINGS
            merge_tree_enable_clear_old_broken_detached=1,
            merge_tree_clear_old_broken_detached_parts_ttl_timeout_seconds=5;
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
        CREATE TABLE
            replicated_mt(date Date, id UInt32, value Int32)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/replicated_mt', '{node1.name}') ORDER BY id
        SETTINGS
            merge_tree_enable_clear_old_broken_detached=1,
            merge_tree_clear_old_broken_detached_parts_ttl_timeout_seconds=5,
            cleanup_delay_period=1,
            cleanup_delay_period_random_add=0;
        """
    )

    for i in range(4):
        node1.query(
            f"INSERT INTO replicated_mt SELECT toDate('2019-10-01'), number, number * number FROM numbers ({i * 100000}, 100000)"
        )

    remove_broken_detached_part_impl("replicated_mt", node1, "broken")
