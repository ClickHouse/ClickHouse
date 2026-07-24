import time

import pytest

import helpers.client
import helpers.cluster
from helpers.corrupt_part_data_on_disk import corrupt_part_data_on_disk

cluster = helpers.cluster.ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/fast_background_pool.xml", "configs/compat.xml"],
    with_zookeeper=True,
    stay_alive=True,
)

node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/fast_background_pool.xml"],
    with_zookeeper=True,
    stay_alive=True,
)

node3 = cluster.add_instance(
    "node3",
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
        # The whole test produces around 6-700 lines, so 2k is plenty enough.
        # wait_for_log_line uses tail + grep, so the overhead is negligible
        look_behind_lines = 2000
        for min_block, max_block in loaded:
            part_name = f"{partition}_{min_block}_{max_block}"
            assert node.wait_for_log_line(
                f"Loading Active part {part_name}", look_behind_lines=look_behind_lines
            )
            assert node.wait_for_log_line(
                f"Finished loading Active part {part_name}",
                look_behind_lines=look_behind_lines,
            )

        failed_part_names = []
        # Let's wait until there is some information about all expected parts, and only
        # check the absence of not expected log messages after all expected logs are present
        for min_block, max_block in failed:
            part_name = f"{partition}_{min_block}_{max_block}"
            failed_part_names.append(part_name)
            assert node.wait_for_log_line(
                f"Loading Active part {part_name}", look_behind_lines=look_behind_lines
            )

        for failed_part_name in failed_part_names:
            assert not node.contains_in_log(
                f"Finished loading Active part {failed_part_name}"
            )

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


def test_merge_tree_load_parts_filesystem_error(started_cluster):
    if node3.is_built_with_sanitizer() or node3.is_debug_build():
        pytest.skip(
            "Skip with debug build and sanitizers. \
            This test intentionally triggers LOGICAL_ERROR which leads to crash with those builds"
        )

    node3.query(
        """
        CREATE TABLE mt_load_parts (id UInt32)
        ENGINE = MergeTree ORDER BY id
        SETTINGS index_granularity_bytes = 0"""
    )

    node3.query("SYSTEM STOP MERGES mt_load_parts")

    for i in range(2):
        node3.query(f"INSERT INTO mt_load_parts VALUES ({i})")

    # We want to somehow check that exception thrown on part creation is handled during part loading.
    # It can be a filesystem exception triggered at initialization of part storage but it hard
    # to trigger it because it should be an exception on stat/listDirectory.
    # The most easy way to trigger such exception is to use chmod but clickhouse server
    # is run with root user in integration test and this won't work. So let's do
    # some stupid things: create a table without adaptive granularity and change mark
    # extensions of data files in part to make clickhouse think that it's a compact part which
    # cannot be created in such table. This will trigger a LOGICAL_ERROR on part creation.

    def corrupt_part(table, part_name):
        part_path = node3.query(
            "SELECT path FROM system.parts WHERE table = '{}' and name = '{}'".format(
                table, part_name
            )
        ).strip()

        node3.exec_in_container(
            ["bash", "-c", f"mv {part_path}id.cmrk {part_path}id.cmrk3"],
            privileged=True,
        )

    corrupt_part("mt_load_parts", "all_1_1_0")
    node3.restart_clickhouse(kill=True)

    assert node3.query("SELECT * FROM mt_load_parts") == "1\n"
    assert (
        node3.query(
            "SELECT name FROM system.detached_parts WHERE table = 'mt_load_parts'"
        )
        == "broken-on-start_all_1_1_0\n"
    )
