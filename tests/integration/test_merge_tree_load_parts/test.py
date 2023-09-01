import pytest
import helpers.client
import helpers.cluster
import time
from helpers.corrupt_part_data_on_disk import corrupt_part_data_on_disk


cluster = helpers.cluster.ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/fast_background_pool.xml"],
    with_zookeeper=True,
    stay_alive=True,
)
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/fast_background_pool.xml"],
    with_zookeeper=True,
    stay_alive=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_merge_tree_load_parts(started_cluster):
    node1.query(
        """
        CREATE TABLE mt_load_parts (pk UInt32, id UInt32, s String)
        ENGINE = MergeTree ORDER BY id PARTITION BY pk"""
    )

    node1.query("SYSTEM STOP MERGES mt_load_parts")

    for i in range(20):
        node1.query(
            f"INSERT INTO mt_load_parts VALUES (44, {i}, randomPrintableASCII(10))"
        )

    node1.restart_clickhouse(kill=True)
    for i in range(1, 21):
        assert node1.contains_in_log(f"Loading Active part 44_{i}_{i}_0")

    node1.query("OPTIMIZE TABLE mt_load_parts FINAL")
    node1.restart_clickhouse(kill=True)

    node1.query("SYSTEM WAIT LOADING PARTS mt_load_parts")

    assert node1.contains_in_log("Loading Active part 44_1_20")
    for i in range(1, 21):
        assert not node1.contains_in_log(f"Loading Active part 44_{i}_{i}_0")
        assert node1.contains_in_log(f"Loading Outdated part 44_{i}_{i}_0")

    assert node1.query("SELECT count() FROM mt_load_parts") == "20\n"

    assert (
        node1.query(
            "SELECT count() FROM system.parts WHERE table = 'mt_load_parts' AND active"
        )
        == "1\n"
    )

    node1.query("ALTER TABLE mt_load_parts MODIFY SETTING old_parts_lifetime = 1")
    node1.query("DETACH TABLE mt_load_parts")
    node1.query("ATTACH TABLE mt_load_parts")

    node1.query("SYSTEM WAIT LOADING PARTS mt_load_parts")

    table_path = node1.query(
        "SELECT data_paths[1] FROM system.tables WHERE table = 'mt_load_parts'"
    ).strip()

    part_dirs = node1.exec_in_container(["bash", "-c", f"ls {table_path}"], user="root")

    part_dirs = list(
        set(part_dirs.strip().split("\n")) - {"detached", "format_version.txt"}
    )

    MAX_RETRY = 10
    part_dirs_ok = False
    for _ in range(MAX_RETRY):
        part_dirs = node1.exec_in_container(
            ["bash", "-c", f"ls {table_path}"], user="root"
        )
        part_dirs = list(
            set(part_dirs.strip().split("\n")) - {"detached", "format_version.txt"}
        )
        part_dirs_ok = len(part_dirs) == 1 and part_dirs[0].startswith("44_1_20")
        if part_dirs_ok:
            break
        time.sleep(2)

    assert part_dirs_ok


def test_merge_tree_load_parts_corrupted(started_cluster):
    for i, node in enumerate([node1, node2]):
        node.query(
            f"""
            CREATE TABLE mt_load_parts_2 (pk UInt32, id UInt32, s String)
            ENGINE = ReplicatedMergeTree('/clickhouse/tables/0/mt_load_parts_2', '{i}') ORDER BY id PARTITION BY pk"""
        )

    """min-max blocks in created parts: 1_1_0, 2_2_0, 1_2_1, 3_3_0, 1_3_2"""
    for partition in [111, 222, 333]:
        node1.query(
            f"INSERT INTO mt_load_parts_2 VALUES ({partition}, 0, randomPrintableASCII(10))"
        )

        node1.query(
            f"INSERT INTO mt_load_parts_2 VALUES ({partition}, 1, randomPrintableASCII(10))"
        )

        node1.query(f"OPTIMIZE TABLE mt_load_parts_2 PARTITION {partition} FINAL")

        node1.query(
            f"INSERT INTO mt_load_parts_2 VALUES ({partition}, 2, randomPrintableASCII(10))"
        )

        node1.query(f"OPTIMIZE TABLE mt_load_parts_2 PARTITION {partition} FINAL")

    node2.query("SYSTEM SYNC REPLICA mt_load_parts_2", timeout=30)

    def get_part_name(node, partition, min_block, max_block):
        return node.query(
            f"""
            SELECT name FROM system.parts
            WHERE table = 'mt_load_parts_2'
            AND partition = '{partition}'
            AND min_block_number = {min_block}
            AND max_block_number = {max_block}"""
        ).strip()

    corrupt_part_data_on_disk(node1, "mt_load_parts_2", get_part_name(node1, 111, 0, 2))
    corrupt_part_data_on_disk(node1, "mt_load_parts_2", get_part_name(node1, 222, 0, 2))
    corrupt_part_data_on_disk(node1, "mt_load_parts_2", get_part_name(node1, 222, 0, 1))
    corrupt_part_data_on_disk(node1, "mt_load_parts_2", get_part_name(node1, 333, 0, 1))
    corrupt_part_data_on_disk(node1, "mt_load_parts_2", get_part_name(node1, 333, 2, 2))

    node1.restart_clickhouse(kill=True)
    node1.query("SYSTEM WAIT LOADING PARTS mt_load_parts_2")

    def check_parts_loading(node, partition, loaded, failed, skipped):
        for min_block, max_block in loaded:
            part_name = f"{partition}_{min_block}_{max_block}"
            assert node.contains_in_log(f"Loading Active part {part_name}")
            assert node.contains_in_log(f"Finished loading Active part {part_name}")

        for min_block, max_block in failed:
            part_name = f"{partition}_{min_block}_{max_block}"
            assert node.contains_in_log(f"Loading Active part {part_name}")
            assert not node.contains_in_log(f"Finished loading Active part {part_name}")

        for min_block, max_block in skipped:
            part_name = f"{partition}_{min_block}_{max_block}"
            assert not node.contains_in_log(f"Loading Active part {part_name}")
            assert not node.contains_in_log(f"Finished loading Active part {part_name}")

    check_parts_loading(
        node1, 111, loaded=[(0, 1), (2, 2)], failed=[(0, 2)], skipped=[(0, 0), (1, 1)]
    )
    check_parts_loading(
        node1, 222, loaded=[(0, 0), (1, 1), (2, 2)], failed=[(0, 2), (0, 1)], skipped=[]
    )
    check_parts_loading(
        node1, 333, loaded=[(0, 2)], failed=[], skipped=[(0, 0), (1, 1), (2, 2), (0, 1)]
    )

    node1.query("SYSTEM SYNC REPLICA mt_load_parts_2", timeout=30)
    node1.query("OPTIMIZE TABLE mt_load_parts_2 FINAL")
    node1.query("SYSTEM SYNC REPLICA mt_load_parts_2", timeout=30)

    assert (
        node1.query(
            """
            SELECT pk, count() FROM mt_load_parts_2
            GROUP BY pk ORDER BY pk"""
        )
        == "111\t3\n222\t3\n333\t3\n"
    )
    assert (
        node1.query(
            """
            SELECT partition, count()
            FROM system.parts WHERE table = 'mt_load_parts_2' AND active
            GROUP BY partition ORDER BY partition"""
        )
        == "111\t1\n222\t1\n333\t1\n"
    )
