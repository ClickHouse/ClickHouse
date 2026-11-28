import os
import time
from multiprocessing.dummy import Pool

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/wide_parts_only.xml"],
    with_remote_database_disk=False,  # The test checks hardlinks in the local disk
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def check_hardlinks(table, part_path, column_file, count):
    column_path = os.path.join(
        "/var/lib/clickhouse/data/default", table, part_path, column_file
    )
    script = """
        export INODE=`ls -i {column_path} | awk '{{print $1}}'`
        export COUNT=`find /var/lib/clickhouse -inum $INODE | wc -l`
        test $COUNT = {count}
    """.format(
        column_path=column_path, count=count
    )

    node1.exec_in_container(["bash", "-c", script])


def check_exists(table, part_path, column_file):
    column_path = os.path.join(
        "/var/lib/clickhouse/data/default", table, part_path, column_file
    )

    node1.exec_in_container(["bash", "-c", "test -f {}".format(column_path)])


def test_update_mutation(started_cluster):
    node1.query("DROP TABLE IF EXISTS table_for_update")
    node1.query(
        "CREATE TABLE table_for_update(key UInt64, value1 UInt64, value2 String) ENGINE MergeTree() ORDER BY tuple()"
    )

    node1.query(
        "INSERT INTO table_for_update SELECT number, number, toString(number) from numbers(100)"
    )

    assert int(node1.query("SELECT sum(value1) FROM table_for_update").strip()) == sum(
        range(100)
    )

    node1.query(
        "ALTER TABLE table_for_update UPDATE value1 = value1 * value1 WHERE 1",
        settings={"mutations_sync": "2"},
    )
    assert int(node1.query("SELECT sum(value1) FROM table_for_update").strip()) == sum(
        i * i for i in range(100)
    )

    check_hardlinks("table_for_update", "all_1_1_0_2", "key.bin", 2)
    check_hardlinks("table_for_update", "all_1_1_0_2", "value2.bin", 2)
    check_hardlinks("table_for_update", "all_1_1_0_2", "value1.bin", 1)

    node1.query(
        "ALTER TABLE table_for_update UPDATE key=key, value1=value1, value2=value2 WHERE 1",
        settings={"mutations_sync": "2"},
    )

    assert int(node1.query("SELECT sum(value1) FROM table_for_update").strip()) == sum(
        i * i for i in range(100)
    )

    check_hardlinks("table_for_update", "all_1_1_0_3", "key.bin", 1)
    check_hardlinks("table_for_update", "all_1_1_0_3", "value1.bin", 1)
    check_hardlinks("table_for_update", "all_1_1_0_3", "value2.bin", 1)

    node1.query("DROP TABLE table_for_update")


def test_modify_mutation(started_cluster):
    node1.query("DROP TABLE IF EXISTS table_for_modify")
    node1.query(
        "CREATE TABLE table_for_modify(key UInt64, value1 UInt64, value2 String) ENGINE MergeTree() ORDER BY tuple()"
    )

    node1.query(
        "INSERT INTO table_for_modify SELECT number, number, toString(number) from numbers(100)"
    )

    assert int(node1.query("SELECT sum(value1) FROM table_for_modify").strip()) == sum(
        range(100)
    )

    node1.query(
        "ALTER TABLE table_for_modify MODIFY COLUMN value2 UInt64",
        settings={"mutations_sync": "2"},
    )

    assert int(node1.query("SELECT sum(value2) FROM table_for_modify").strip()) == sum(
        range(100)
    )

    check_hardlinks("table_for_modify", "all_1_1_0_2", "key.bin", 2)
    check_hardlinks("table_for_modify", "all_1_1_0_2", "value1.bin", 2)
    check_hardlinks("table_for_modify", "all_1_1_0_2", "value2.bin", 1)

    node1.query("DROP TABLE table_for_modify")


def test_drop_mutation(started_cluster):
    node1.query("DROP TABLE IF EXISTS table_for_drop")
    node1.query(
        "CREATE TABLE table_for_drop(key UInt64, value1 UInt64, value2 String) ENGINE MergeTree() ORDER BY tuple()"
    )

    node1.query(
        "INSERT INTO table_for_drop SELECT number, number, toString(number) from numbers(100)"
    )

    assert int(node1.query("SELECT sum(value1) FROM table_for_drop").strip()) == sum(
        range(100)
    )

    node1.query(
        "ALTER TABLE table_for_drop DROP COLUMN value2",
        settings={"mutations_sync": "2"},
    )

    check_hardlinks("table_for_drop", "all_1_1_0_2", "key.bin", 2)
    check_hardlinks("table_for_drop", "all_1_1_0_2", "value1.bin", 2)

    with pytest.raises(Exception):
        check_exists("table_for_drop", "all_1_1_0_2", "value2.bin")
    with pytest.raises(Exception):
        check_exists("table_for_drop", "all_1_1_0_2", "value2.mrk")

    node1.query("DROP TABLE table_for_drop")


def test_delete_and_drop_mutation(started_cluster):
    node1.query("DROP TABLE IF EXISTS table_for_delete_and_drop")
    node1.query(
        "CREATE TABLE table_for_delete_and_drop(key UInt64, value1 UInt64, value2 String) ENGINE MergeTree() ORDER BY tuple()"
    )

    node1.query(
        "INSERT INTO table_for_delete_and_drop SELECT number, number, toString(number) from numbers(100)"
    )

    assert int(
        node1.query("SELECT sum(value1) FROM table_for_delete_and_drop").strip()
    ) == sum(range(100))

    node1.query("SYSTEM STOP MERGES")

    def mutate():
        node1.query(
            "ALTER TABLE table_for_delete_and_drop DELETE WHERE key % 2 == 0, DROP COLUMN value2"
        )

    p = Pool(2)
    p.apply_async(mutate)

    for _ in range(1, 100):
        result = node1.query(
            "SELECT COUNT() FROM system.mutations WHERE table = 'table_for_delete_and_drop' and is_done=0"
        )
        try:
            if int(result.strip()) == 2:
                break
        except:
            print("Result", result)
            pass

        time.sleep(0.5)

    node1.query("SYSTEM START MERGES")

    assert_eq_with_retry(
        node1,
        "SELECT COUNT() FROM table_for_delete_and_drop",
        str(sum(1 for i in range(100) if i % 2 != 0)),
    )

    check_hardlinks("table_for_delete_and_drop", "all_1_1_0_3", "key.bin", 1)
    check_hardlinks("table_for_delete_and_drop", "all_1_1_0_3", "value1.bin", 1)

    with pytest.raises(Exception):
        check_exists("table_for_delete_and_drop", "all_1_1_0_3", "value2.bin")
    with pytest.raises(Exception):
        check_exists("table_for_delete_and_drop", "all_1_1_0_3", "value2.mrk")

    node1.query("DROP TABLE table_for_delete_and_drop")


def get_active_part_path(table):
    return node1.query(
        f"SELECT path FROM system.parts WHERE database = 'default' AND table = '{table}' AND active"
    ).strip()


def get_payload_inodes(part_path):
    files = node1.exec_in_container(
        ["find", part_path, "-type", "f", "-printf", "%P\t%D:%i\n"]
    )
    return {
        name: inode
        for name, inode in (line.split("\t") for line in files.splitlines())
        if (
            name.endswith((".bin", ".idx", ".idx2", ".cidx"))
            or os.path.basename(name) == "skp_idx.packed"
            or os.path.splitext(name)[1].startswith((".mrk", ".cmrk"))
        )
        and not os.path.basename(name).startswith("minmax_")
    }


@pytest.mark.parametrize("initial_ttl", [False, True])
@pytest.mark.parametrize("ttl_only_drop_parts", [0, 1])
def test_compact_ttl_recalculate_hardlinks(
    started_cluster, initial_ttl, ttl_only_drop_parts
):
    table = f"compact_ttl_{int(initial_ttl)}_{ttl_only_drop_parts}"
    node1.query(f"""
        CREATE TABLE {table}
        (
            k UInt64,
            ts DateTime('UTC'),
            payload String,
            v UInt64,
            INDEX v_idx v TYPE minmax GRANULARITY 1,
            PROJECTION by_v (SELECT * ORDER BY v)
        ) ENGINE = MergeTree ORDER BY k
        {"TTL ts + INTERVAL 100 YEAR" if initial_ttl else ""}
        SETTINGS min_bytes_for_wide_part = '10G', min_rows_for_wide_part = 1000000000,
                 min_bytes_for_full_part_storage = 0, min_rows_for_full_part_storage = 0,
                 materialize_ttl_recalculate_only = 1,
                 ttl_only_drop_parts = {ttl_only_drop_parts},
                 max_number_of_merges_with_ttl_in_pool = 0, old_parts_lifetime = 3600,
                 enable_block_number_column = 1, enable_block_offset_column = 1,
                 index_granularity = 16, index_granularity_bytes = 1048576
        """)
    try:
        node1.query(
            f"INSERT INTO {table} SELECT number, toDateTime('2000-01-01', 'UTC') + number, "
            "if(number % 16 = 0, repeat('payload', 100), ''), 127 - number FROM numbers(128)"
        )
        assert (
            node1.query(
                f"SELECT part_type, part_storage_type FROM system.parts WHERE table = '{table}' AND active"
            )
            == "Compact\tFull\n"
        )
        source_path = get_active_part_path(table)
        source_inodes = get_payload_inodes(source_path)
        assert "data.bin" in source_inodes
        assert any("mrk" in name for name in source_inodes)
        assert any(name.startswith("by_v.proj/") for name in source_inodes)
        source_ttl = (
            node1.exec_in_container(["cat", source_path + "ttl.txt"])
            if initial_ttl
            else None
        )
        contents_query = f"SELECT k, ts, payload, v, _block_number, _block_offset FROM {table} ORDER BY k"
        contents = node1.query(contents_query)

        # Exercise automatic materialization, then an explicit `MATERIALIZE TTL`.
        node1.query(
            f"ALTER TABLE {table} MODIFY TTL ts + INTERVAL 1 YEAR",
            settings={"mutations_sync": 2, "materialize_ttl_after_modify": 1},
        )
        assert get_active_part_path(table) != source_path
        assert get_payload_inodes(get_active_part_path(table)) == source_inodes
        assert node1.query(contents_query) == contents
        assert (
            node1.query(
                f"SELECT toTimeZone(delete_ttl_info_min, 'UTC'), toTimeZone(delete_ttl_info_max, 'UTC') "
                f"FROM system.parts WHERE table = '{table}' AND active"
            )
            == "2001-01-01 00:00:00\t2001-01-01 00:02:07\n"
        )

        node1.query(
            f"ALTER TABLE {table} MODIFY TTL ts + INTERVAL 2 YEAR",
            settings={"materialize_ttl_after_modify": 0},
        )
        node1.query(
            f"ALTER TABLE {table} MATERIALIZE TTL", settings={"mutations_sync": 2}
        )
        assert get_payload_inodes(get_active_part_path(table)) == source_inodes
        assert node1.query(contents_query) == contents
        assert (
            node1.query(
                f"SELECT toTimeZone(delete_ttl_info_min, 'UTC'), toTimeZone(delete_ttl_info_max, 'UTC') "
                f"FROM system.parts WHERE table = '{table}' AND active"
            )
            == "2002-01-01 00:00:00\t2002-01-01 00:02:07\n"
        )
        if initial_ttl:
            assert (
                node1.exec_in_container(["cat", source_path + "ttl.txt"]) == source_ttl
            )

        node1.query(
            f"ALTER TABLE {table} MODIFY COLUMN payload String TTL ts + INTERVAL 1 YEAR",
            settings={"materialize_ttl_after_modify": 0},
        )
        node1.query(
            f"ALTER TABLE {table} MATERIALIZE TTL", settings={"mutations_sync": 2}
        )
        assert get_payload_inodes(get_active_part_path(table)) == source_inodes
        assert node1.query(contents_query) == contents
        assert (
            node1.query(
                f"CHECK TABLE {table}", settings={"check_query_single_value_result": 1}
            )
            == "1\n"
        )

        # The TTL reader must evaluate defaults without materializing their columns.
        node1.query(
            f"ALTER TABLE {table} ADD COLUMN expires DateTime('UTC') DEFAULT ts + INTERVAL 1 YEAR"
        )
        node1.query(
            f"ALTER TABLE {table} MODIFY TTL expires + INTERVAL 2 YEAR",
            settings={"materialize_ttl_after_modify": 0},
        )
        node1.query(
            f"ALTER TABLE {table} MATERIALIZE TTL", settings={"mutations_sync": 2}
        )
        assert get_payload_inodes(get_active_part_path(table)) == source_inodes
        assert (
            node1.query(
                f"SELECT toTimeZone(delete_ttl_info_min, 'UTC'), toTimeZone(delete_ttl_info_max, 'UTC') "
                f"FROM system.parts WHERE table = '{table}' AND active"
            )
            == "2003-01-01 00:00:00\t2003-01-01 00:02:07\n"
        )

        node1.query(f"DETACH TABLE {table} SYNC")
        node1.query(f"ATTACH TABLE {table}")
        assert node1.query(contents_query) == contents
        assert (
            node1.query(
                f"CHECK TABLE {table}", settings={"check_query_single_value_result": 1}
            )
            == "1\n"
        )
        assert (
            node1.query(
                f"SELECT count() FROM {table} WHERE v = 5 "
                "SETTINGS force_data_skipping_indices = 'v_idx', optimize_use_projections = 0"
            )
            == "1\n"
        )
    finally:
        node1.query(f"DROP TABLE IF EXISTS {table} SYNC")


def test_compact_ttl_recalculate_mixed_mutation(started_cluster):
    table = "compact_ttl_mixed"
    node1.query(f"""
        CREATE TABLE {table} (k UInt64, ts DateTime('UTC'), v UInt64)
        ENGINE = MergeTree ORDER BY k TTL ts + INTERVAL 100 YEAR
        SETTINGS min_bytes_for_wide_part = '10G', min_rows_for_wide_part = 1000000000,
                 min_bytes_for_full_part_storage = 0, min_rows_for_full_part_storage = 0,
                 materialize_ttl_recalculate_only = 1,
                 max_number_of_merges_with_ttl_in_pool = 0, old_parts_lifetime = 3600
        """)
    try:
        node1.query(
            f"INSERT INTO {table} SELECT number, toDateTime('2000-01-01', 'UTC'), number FROM numbers(10)"
        )
        source_inode = get_payload_inodes(get_active_part_path(table))["data.bin"]
        node1.query(
            f"ALTER TABLE {table} UPDATE v = v + 1 WHERE 1, MATERIALIZE TTL",
            settings={"mutations_sync": 2},
        )
        assert node1.query(f"SELECT count(), sum(v) FROM {table}") == "10\t55\n"
        assert (
            get_payload_inodes(get_active_part_path(table))["data.bin"] != source_inode
        )

        node1.query(
            f"ALTER TABLE {table} MODIFY SETTING materialize_ttl_recalculate_only = 0"
        )
        node1.query(
            f"ALTER TABLE {table} MODIFY TTL ts + INTERVAL 1 YEAR",
            settings={"materialize_ttl_after_modify": 0},
        )
        node1.query(
            f"ALTER TABLE {table} MATERIALIZE TTL", settings={"mutations_sync": 2}
        )
        assert node1.query(f"SELECT count() FROM {table}") == "0\n"
        assert (
            node1.query(
                f"CHECK TABLE {table}", settings={"check_query_single_value_result": 1}
            )
            == "1\n"
        )
    finally:
        node1.query(f"DROP TABLE IF EXISTS {table} SYNC")


@pytest.mark.parametrize("recalculate_only", [0, 1])
@pytest.mark.parametrize("ttl_only_drop_parts", [0, 1])
def test_compact_ttl_recalculate_setting_snapshot(
    started_cluster, recalculate_only, ttl_only_drop_parts
):
    table = f"ttl_mode_snapshot_{recalculate_only}_{ttl_only_drop_parts}"
    failpoint = "mt_mutate_task_pause_after_ttl_mode_selection"
    node1.query(f"""
        CREATE TABLE {table} (k UInt64, ts DateTime('UTC'))
        ENGINE = MergeTree ORDER BY k TTL ts + INTERVAL 1 YEAR
        SETTINGS min_bytes_for_wide_part = '10G', min_rows_for_wide_part = 1000000000,
                 min_bytes_for_full_part_storage = 0, min_rows_for_full_part_storage = 0,
                 materialize_ttl_recalculate_only = {recalculate_only},
                 ttl_only_drop_parts = {ttl_only_drop_parts},
                 max_number_of_merges_with_ttl_in_pool = 0
        """)
    try:
        node1.query(
            f"INSERT INTO {table} SELECT number, toDateTime('2000-01-01', 'UTC') FROM numbers(10)"
        )
        node1.query(f"SYSTEM ENABLE FAILPOINT {failpoint}")
        node1.query(
            f"ALTER TABLE {table} MATERIALIZE TTL", settings={"mutations_sync": 0}
        )
        node1.query(f"SYSTEM WAIT FAILPOINT {failpoint} PAUSE", timeout=30)
        node1.query(
            f"ALTER TABLE {table} MODIFY SETTING materialize_ttl_recalculate_only = {1 - recalculate_only}"
        )
        node1.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")
        assert_eq_with_retry(
            node1,
            f"SELECT count() FROM system.mutations WHERE database = 'default' AND table = '{table}' AND NOT is_done",
            "0",
        )
        assert node1.query(f"SELECT count() FROM {table}") == (
            "10\n" if recalculate_only else "0\n"
        )
        if recalculate_only:
            assert (
                node1.query(
                    f"SELECT toTimeZone(delete_ttl_info_min, 'UTC'), toTimeZone(delete_ttl_info_max, 'UTC') "
                    f"FROM system.parts WHERE table = '{table}' AND active"
                )
                == "2001-01-01 00:00:00\t2001-01-01 00:00:00\n"
            )
        assert (
            node1.query(
                f"CHECK TABLE {table}", settings={"check_query_single_value_result": 1}
            )
            == "1\n"
        )
    finally:
        node1.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")
        node1.query(f"DROP TABLE IF EXISTS {table} SYNC")


@pytest.mark.parametrize("ttl_only_drop_parts", [0, 1])
@pytest.mark.parametrize("all_expired", [False, True])
def test_compact_ttl_drop_parts_setting_snapshot(
    started_cluster, ttl_only_drop_parts, all_expired
):
    table = f"ttl_drop_mode_snapshot_{ttl_only_drop_parts}_{int(all_expired)}"
    failpoint = "mt_mutate_task_pause_after_ttl_mode_selection"
    node1.query(f"""
        CREATE TABLE {table} (k UInt64, ts DateTime('UTC'))
        ENGINE = MergeTree ORDER BY k TTL ts + INTERVAL 1 YEAR
        SETTINGS min_bytes_for_wide_part = '10G', min_rows_for_wide_part = 1000000000,
                 min_bytes_for_full_part_storage = 0, min_rows_for_full_part_storage = 0,
                 materialize_ttl_recalculate_only = 0,
                 ttl_only_drop_parts = {ttl_only_drop_parts},
                 max_number_of_merges_with_ttl_in_pool = 0
        """)
    try:
        expired_condition = "1" if all_expired else "number < 5"
        node1.query(
            f"INSERT INTO {table} SELECT number, if({expired_condition}, "
            "toDateTime('2000-01-01', 'UTC'), toDateTime('2090-01-01', 'UTC')) FROM numbers(10)"
        )
        node1.query(f"SYSTEM ENABLE FAILPOINT {failpoint}")
        node1.query(
            f"ALTER TABLE {table} MATERIALIZE TTL", settings={"mutations_sync": 0}
        )
        node1.query(f"SYSTEM WAIT FAILPOINT {failpoint} PAUSE", timeout=30)
        node1.query(
            f"ALTER TABLE {table} MODIFY SETTING ttl_only_drop_parts = {1 - ttl_only_drop_parts}"
        )
        node1.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")
        assert_eq_with_retry(
            node1,
            f"SELECT count() FROM system.mutations WHERE database = 'default' AND table = '{table}' AND NOT is_done",
            "0",
        )

        # A fully expired part is removed in either mode. Only row-level TTL removes half a part.
        expected_rows = 0 if all_expired else (10 if ttl_only_drop_parts else 5)
        assert node1.query(f"SELECT count() FROM {table}") == f"{expected_rows}\n"
        assert (
            node1.query(
                f"CHECK TABLE {table}", settings={"check_query_single_value_result": 1}
            )
            == "1\n"
        )
    finally:
        node1.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")
        node1.query(f"DROP TABLE IF EXISTS {table} SYNC")
