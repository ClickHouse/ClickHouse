import pytest

from helpers.cluster import ClickHouseCluster
from helpers.corrupt_part_data_on_disk import corrupt_part_data_on_disk
from helpers.corrupt_part_data_on_disk import break_part
import time

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1", stay_alive=True, main_configs=["configs/store_cleanup.xml"]
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


def test_store_cleanup(started_cluster):
    node1.query("CREATE DATABASE db UUID '10000000-1000-4000-8000-000000000001'")
    node1.query(
        "CREATE TABLE db.log UUID '10000000-1000-4000-8000-000000000002' ENGINE=Log AS SELECT 1"
    )
    node1.query(
        "CREATE TABLE db.mt UUID '10000000-1000-4000-8000-000000000003' ENGINE=MergeTree ORDER BY tuple() AS SELECT 1"
    )
    node1.query(
        "CREATE TABLE db.mem UUID '10000000-1000-4000-8000-000000000004' ENGINE=Memory AS SELECT 1"
    )

    node1.query("CREATE DATABASE db2 UUID '20000000-1000-4000-8000-000000000001'")
    node1.query(
        "CREATE TABLE db2.log UUID '20000000-1000-4000-8000-000000000002' ENGINE=Log AS SELECT 1"
    )
    node1.query("DETACH DATABASE db2")

    node1.query("CREATE DATABASE db3 UUID '30000000-1000-4000-8000-000000000001'")
    node1.query(
        "CREATE TABLE db3.log UUID '30000000-1000-4000-8000-000000000002' ENGINE=Log AS SELECT 1"
    )
    node1.query(
        "CREATE TABLE db3.log2 UUID '30000000-1000-4000-8000-000000000003' ENGINE=Log AS SELECT 1"
    )
    node1.query("DETACH TABLE db3.log")
    node1.query("DETACH TABLE db3.log2 PERMANENTLY")

    assert "d---------" not in node1.exec_in_container(
        ["ls", "-l", f"{path_to_data}/store"]
    )
    assert "d---------" not in node1.exec_in_container(
        ["ls", "-l", f"{path_to_data}/store/100"]
    )
    assert "d---------" not in node1.exec_in_container(
        ["ls", "-l", f"{path_to_data}/store/200"]
    )
    assert "d---------" not in node1.exec_in_container(
        ["ls", "-l", f"{path_to_data}/store/300"]
    )

    node1.stop_clickhouse(kill=True)
    # All dirs related to `db` will be removed
    node1.exec_in_container(["rm", f"{path_to_data}/metadata/db.sql"])

    node1.exec_in_container(["mkdir", f"{path_to_data}/store/kek"])
    node1.exec_in_container(["touch", f"{path_to_data}/store/12"])
    node1.exec_in_container(["mkdir", f"{path_to_data}/store/456"])
    node1.exec_in_container(["mkdir", f"{path_to_data}/store/456/testgarbage"])
    node1.exec_in_container(
        ["mkdir", f"{path_to_data}/store/456/30000000-1000-4000-8000-000000000003"]
    )
    node1.exec_in_container(
        ["touch", f"{path_to_data}/store/456/45600000-1000-4000-8000-000000000003"]
    )
    node1.exec_in_container(
        ["mkdir", f"{path_to_data}/store/456/45600000-1000-4000-8000-000000000004"]
    )

    node1.start_clickhouse()
    node1.query("DETACH DATABASE db2")
    node1.query("DETACH TABLE db3.log")

    node1.wait_for_log_line(
        "Removing access rights for unused directory",
        timeout=60,
        look_behind_lines=1000,
    )
    node1.wait_for_log_line("directories from store")

    store = node1.exec_in_container(["ls", f"{path_to_data}/store"])
    assert "100" in store
    assert "200" in store
    assert "300" in store
    assert "456" in store
    assert "kek" in store
    assert "12" in store
    assert "d---------" in node1.exec_in_container(
        ["ls", "-l", f"{path_to_data}/store"]
    )
    assert "d---------" in node1.exec_in_container(
        ["ls", "-l", f"{path_to_data}/store/456"]
    )

    # Metadata is removed, so store/100 contains garbage
    store100 = node1.exec_in_container(["ls", f"{path_to_data}/store/100"])
    assert "10000000-1000-4000-8000-000000000001" in store100
    assert "10000000-1000-4000-8000-000000000002" in store100
    assert "10000000-1000-4000-8000-000000000003" in store100
    assert "d---------" in node1.exec_in_container(
        ["ls", "-l", f"{path_to_data}/store/100"]
    )

    # Database is detached, nothing to clean up
    store200 = node1.exec_in_container(["ls", f"{path_to_data}/store/200"])
    assert "20000000-1000-4000-8000-000000000001" in store200
    assert "20000000-1000-4000-8000-000000000002" in store200
    assert "d---------" not in node1.exec_in_container(
        ["ls", "-l", f"{path_to_data}/store/200"]
    )

    # Tables are detached, nothing to clean up
    store300 = node1.exec_in_container(["ls", f"{path_to_data}/store/300"])
    assert "30000000-1000-4000-8000-000000000001" in store300
    assert "30000000-1000-4000-8000-000000000002" in store300
    assert "30000000-1000-4000-8000-000000000003" in store300
    assert "d---------" not in node1.exec_in_container(
        ["ls", "-l", f"{path_to_data}/store/300"]
    )

    # Manually created garbage
    store456 = node1.exec_in_container(["ls", f"{path_to_data}/store/456"])
    assert "30000000-1000-4000-8000-000000000003" in store456
    assert "45600000-1000-4000-8000-000000000003" in store456
    assert "45600000-1000-4000-8000-000000000004" in store456
    assert "testgarbage" in store456
    assert "----------" in node1.exec_in_container(
        ["ls", "-l", f"{path_to_data}/store/456"]
    )

    node1.wait_for_log_line(
        "Removing unused directory", timeout=90, look_behind_lines=1000
    )
    node1.wait_for_log_line("directories from store")

    store = node1.exec_in_container(["ls", f"{path_to_data}/store"])
    assert "100" in store
    assert "200" in store
    assert "300" in store
    assert "456" in store
    assert "kek" not in store  # changed
    assert "\n12\n" not in store  # changed
    assert "d---------" not in node1.exec_in_container(
        ["ls", "-l", f"{path_to_data}/store"]
    )  # changed

    # Metadata is removed, so store/100 contains garbage
    store100 = node1.exec_in_container(["ls", f"{path_to_data}/store/100"])  # changed
    assert "10000000-1000-4000-8000-000000000001" not in store100  # changed
    assert "10000000-1000-4000-8000-000000000002" not in store100  # changed
    assert "10000000-1000-4000-8000-000000000003" not in store100  # changed
    assert "d---------" not in node1.exec_in_container(
        ["ls", "-l", f"{path_to_data}/store/100"]
    )  # changed

    # Database is detached, nothing to clean up
    store200 = node1.exec_in_container(["ls", f"{path_to_data}/store/200"])
    assert "20000000-1000-4000-8000-000000000001" in store200
    assert "20000000-1000-4000-8000-000000000002" in store200
    assert "d---------" not in node1.exec_in_container(
        ["ls", "-l", f"{path_to_data}/store/200"]
    )

    # Tables are detached, nothing to clean up
    store300 = node1.exec_in_container(["ls", f"{path_to_data}/store/300"])
    assert "30000000-1000-4000-8000-000000000001" in store300
    assert "30000000-1000-4000-8000-000000000002" in store300
    assert "30000000-1000-4000-8000-000000000003" in store300
    assert "d---------" not in node1.exec_in_container(
        ["ls", "-l", f"{path_to_data}/store/300"]
    )

    # Manually created garbage
    store456 = node1.exec_in_container(["ls", f"{path_to_data}/store/456"])
    assert "30000000-1000-4000-8000-000000000003" not in store456  # changed
    assert "45600000-1000-4000-8000-000000000003" not in store456  # changed
    assert "45600000-1000-4000-8000-000000000004" not in store456  # changed
    assert "testgarbage" not in store456  # changed
    assert "---------" not in node1.exec_in_container(
        ["ls", "-l", f"{path_to_data}/store/456"]
    )  # changed

    node1.query("ATTACH TABLE db3.log2")
    node1.query("ATTACH DATABASE db2")
    node1.query("ATTACH TABLE db3.log")

    assert "1\n" == node1.query("SELECT * FROM db3.log")
    assert "1\n" == node1.query("SELECT * FROM db3.log2")
    assert "1\n" == node1.query("SELECT * FROM db2.log")
