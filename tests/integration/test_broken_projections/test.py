import time
import pytest
import logging
import string
import random
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "node",
            main_configs=["config.d/backups.xml"],
            stay_alive=True,
            with_zookeeper=True,
        )

        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        yield cluster
    finally:
        cluster.shutdown()


def create_table(node, table, replica, data_prefix="", aggressive_merge=True):
    if data_prefix == "":
        data_prefix = table

    if aggressive_merge:
        vertical_merge_algorithm_min_rows_to_activate = 1
        vertical_merge_algorithm_min_columns_to_activate = 1
        max_parts_to_merge_at_once = 3
    else:
        vertical_merge_algorithm_min_rows_to_activate = 100000
        vertical_merge_algorithm_min_columns_to_activate = 100
        max_parts_to_merge_at_once = 3

    node.query(
        f"""
    DROP TABLE IF EXISTS {table} SYNC;
    CREATE TABLE {table}
    (
        a String,
        b String,
        c Int64,
        d Int64,
        e Int64,
        PROJECTION proj1
        (
            SELECT c ORDER BY d
        ),
        PROJECTION proj2
        (
            SELECT d ORDER BY c
        )
    )
    ENGINE = ReplicatedMergeTree('/test_broken_projection_{data_prefix}/data/', '{replica}') ORDER BY a
    SETTINGS min_bytes_for_wide_part = 0,
        max_parts_to_merge_at_once={max_parts_to_merge_at_once},
        enable_vertical_merge_algorithm=0,
        vertical_merge_algorithm_min_rows_to_activate = {vertical_merge_algorithm_min_rows_to_activate},
        vertical_merge_algorithm_min_columns_to_activate = {vertical_merge_algorithm_min_columns_to_activate},
        compress_primary_key=0;
    """
    )


def insert(node, table, offset, size):
    node.query(
        f"""
        INSERT INTO {table}
        SELECT number, number, number, number, number%2 FROM numbers({offset}, {size})
        SETTINGS insert_keeper_fault_injection_probability=0.0;
        """
    )


def get_parts(node, table):
    return (
        node.query(
            f"""
    SELECT name
    FROM system.parts
    WHERE table='{table}' AND database=currentDatabase() AND active = 1
    ORDER BY name;"
    """
        )
        .strip()
        .split("\n")
    )


def bash(node, command):
    node.exec_in_container(["bash", "-c", command], privileged=True, user="root")


def break_projection(node, table, part, parent_part, break_type):
    part_path = node.query(
        f"""
    SELECT path
    FROM system.projection_parts
    WHERE table='{table}'
    AND database=currentDatabase()
    AND active=1
    AND part_name='{part}'
    AND parent_name='{parent_part}'
    ORDER BY modification_time DESC
    LIMIT 1;
    """
    ).strip()

    node.query(
        f"select throwIf(substring('{part_path}', 1, 1) != '/', 'Path is relative: {part_path}')"
    )

    if break_type == "data":
        bash(node, f"rm '{part_path}/d.bin'")
        bash(node, f"rm '{part_path}/c.bin'")
    elif break_type == "metadata":
        bash(node, f"rm '{part_path}/columns.txt'")
    elif break_type == "part":
        bash(node, f"rm -r '{part_path}'")


def break_part(node, table, part):
    part_path = node.query(
        f"""
    SELECT path
    FROM system.parts
    WHERE table='{table}'
    AND database=currentDatabase()
    AND active=1
    AND part_name='{part}'
    ORDER BY modification_time DESC
    LIMIT 1;
    """
    ).strip()

    node.query(
        f"select throwIf(substring('{part_path}', 1, 1) != '/', 'Path is relative: {part_path}')"
    )
    bash(node, f"rm '{part_path}/columns.txt'")


def get_broken_projections_info(node, table, active=True):
    return node.query(
        f"""
    SELECT parent_name, name, errors.name FROM
    (
        SELECT parent_name, name, exception_code
        FROM system.projection_parts
        WHERE table='{table}'
        AND database=currentDatabase()
        AND is_broken = 1
        AND active = {active}
    ) AS parts_info
    INNER JOIN system.errors AS errors
    ON parts_info.exception_code = errors.code
    ORDER BY parent_name, name
    """
    ).strip()


def get_projections_info(node, table):
    return node.query(
        f"""
    SELECT parent_name, name, is_broken
    FROM system.projection_parts
    WHERE table='{table}'
    AND active = 1
    AND database=currentDatabase()
    ORDER BY parent_name, name
    """
    ).strip()


def optimize(node, table, final, no_wait):
    query = f"OPTIMIZE TABLE {table}"
    if final:
        query += " FINAL"
    if no_wait:
        query += " SETTINGS alter_sync=0"
    node.query(query)


def reattach(node, table):
    node.query(
        f"""
    DETACH TABLE {table};
    ATTACH TABLE {table};
    """
    )


def materialize_projection(node, table, proj):
    node.query(
        f"ALTER TABLE {table} MATERIALIZE PROJECTION {proj} SETTINGS mutations_sync=2"
    )


def check_table_full(node, table):
    return node.query(
        f"CHECK TABLE {table} SETTINGS check_query_single_value_result = 0;"
    ).strip()


def random_str(length=6):
    alphabet = string.ascii_lowercase + string.digits
    return "".join(random.SystemRandom().choice(alphabet) for _ in range(length))


def check(
    node,
    table,
    check_result,
    expect_broken_part="",
    expected_error="",
    do_check_command=True,
):
    if expect_broken_part == "proj1":
        assert expected_error in node.query_and_get_error(
            f"SELECT c FROM '{table}' WHERE d == 12 ORDER BY c SETTINGS force_optimize_projection_name = 'proj1'"
        )
    else:
        query_id = node.query(
            f"SELECT queryID() FROM (SELECT c FROM '{table}' WHERE d == 12 ORDER BY c SETTINGS force_optimize_projection_name = 'proj1')"
        ).strip()
        for _ in range(10):
            node.query("SYSTEM FLUSH LOGS")
            res = node.query(
                f"""
            SELECT query, splitByChar('.', arrayJoin(projections))[-1]
            FROM system.query_log
            WHERE query_id='{query_id}' AND type='QueryFinish'
            """
            )
            if res != "":
                break
        if res == "":
            res = node.query(
                """
                SELECT query_id, query, splitByChar('.', arrayJoin(projections))[-1]
                FROM system.query_log ORDER BY query_start_time_microseconds DESC
            """
            )
            print(f"Looked for query id {query_id}, but to no avail: {res}")
            assert False
        assert "proj1" in res

    if expect_broken_part == "proj2":
        assert expected_error in node.query_and_get_error(
            f"SELECT d FROM '{table}' WHERE c == 12 ORDER BY d SETTINGS force_optimize_projection_name = 'proj2'"
        )
    else:
        query_id = node.query(
            f"SELECT queryID() FROM (SELECT d FROM '{table}' WHERE c == 12 ORDER BY d SETTINGS force_optimize_projection_name = 'proj2')"
        ).strip()
        for _ in range(10):
            node.query("SYSTEM FLUSH LOGS")
            res = node.query(
                f"""
            SELECT query, splitByChar('.', arrayJoin(projections))[-1]
            FROM system.query_log
            WHERE query_id='{query_id}' AND type='QueryFinish'
            """
            )
            if res != "":
                break
        if res == "":
            res = node.query(
                """
                SELECT query_id, query, splitByChar('.', arrayJoin(projections))[-1]
                FROM system.query_log ORDER BY query_start_time_microseconds DESC
            """
            )
            print(f"Looked for query id {query_id}, but to no avail: {res}")
            assert False
        assert "proj2" in res

    if do_check_command:
        assert check_result == int(node.query(f"CHECK TABLE {table}"))


def test_broken_ignored(cluster):
    node = cluster.instances["node"]

    table_name = "test1"
    create_table(node, table_name, 1)

    insert(node, table_name, 0, 5)
    insert(node, table_name, 5, 5)
    insert(node, table_name, 10, 5)
    insert(node, table_name, 15, 5)

    assert ["all_0_0_0", "all_1_1_0", "all_2_2_0", "all_3_3_0"] == get_parts(
        node, table_name
    )

    # Break metadata (columns.txt) file of projection 'proj1'
    break_projection(node, table_name, "proj1", "all_2_2_0", "metadata")

    # Do select and after "check table" query.
    # Select works because it does not read columns.txt.
    # But expect check table result as 0.
    check(node, table_name, 0)

    # Projection 'proj1' from part all_2_2_0 will now appear in broken parts info
    # because it was marked broken during "check table" query.
    assert "all_2_2_0\tproj1\tFILE_DOESNT_EXIST" in get_broken_projections_info(
        node, table_name
    )

    # Check table query will also show a list of parts which have broken projections.
    assert "all_2_2_0" in check_table_full(node, table_name)

    # Break data file of projection 'proj2' for part all_2_2_0
    break_projection(node, table_name, "proj2", "all_2_2_0", "data")

    # It will not yet appear in broken projections info.
    assert "proj2" not in get_broken_projections_info(node, table_name)

    # Select now fails with error "File doesn't exist"
    check(node, table_name, 0, "proj2", "FILE_DOESNT_EXIST")

    # Projection 'proj2' from part all_2_2_0 will now appear in broken parts info.
    assert "all_2_2_0\tproj2\tNO_FILE_IN_DATA_PART" in get_broken_projections_info(
        node, table_name
    )

    # Second select works, because projection is now marked as broken.
    check(node, table_name, 0)

    # Break data file of projection 'proj2' for part all_3_3_0
    break_projection(node, table_name, "proj2", "all_3_3_0", "data")

    # It will not yet appear in broken projections info.
    assert "all_3_3_0" not in get_broken_projections_info(node, table_name)

    insert(node, table_name, 20, 5)
    insert(node, table_name, 25, 5)

    # Part all_3_3_0 has 'proj' and 'proj2' projections, but 'proj2' is broken and server does NOT know it yet.
    # Parts all_4_4_0 and all_5_5_0 have both non-broken projections.
    # So a merge will be create for future part all_3_5_1.
    # During merge it will fail to read from 'proj2' of part all_3_3_0 and proj2 will be marked broken.
    # Merge will be retried and on second attempt it will succeed.
    # The result part all_3_5_1 will have only 1 projection - 'proj', because
    # it will skip 'proj2' as it will see that one part does not have it anymore in the set of valid projections.
    optimize(node, table_name, 0, 1)
    time.sleep(5)

    # table_uuid=node.query(f"SELECT uuid FROM system.tables WHERE table='{table_name}' and database=currentDatabase()").strip()
    # assert 0 < int(
    #    node.query(
    #        f"""
    # SYSTEM FLUSH LOGS;
    # SELECT count() FROM system.text_log
    # WHERE level='Error'
    # AND logger_name='MergeTreeBackgroundExecutor'
    # AND message like 'Exception while executing background task %{table_uuid}:all_3_5_1%%Cannot open file%proj2.proj/c.bin%'
    # """)
    # )

    assert ["all_0_0_0", "all_1_1_0", "all_2_2_0", "all_3_5_1"] == get_parts(
        node, table_name
    )

    assert "all_3_3_0" in get_broken_projections_info(node, table_name, active=False)
    assert "all_2_2_0" in get_broken_projections_info(node, table_name, active=True)

    # 0 because of all_2_2_0
    check(node, table_name, 0)


def test_materialize_broken_projection(cluster):
    node = cluster.instances["node"]

    table_name = "test2"
    create_table(node, table_name, 1)

    insert(node, table_name, 0, 5)
    insert(node, table_name, 5, 5)
    insert(node, table_name, 10, 5)
    insert(node, table_name, 15, 5)

    assert ["all_0_0_0", "all_1_1_0", "all_2_2_0", "all_3_3_0"] == get_parts(
        node, table_name
    )

    break_projection(node, table_name, "proj1", "all_1_1_0", "metadata")
    reattach(node, table_name)

    assert "all_1_1_0\tproj1\tNO_FILE_IN_DATA_PART" in get_broken_projections_info(
        node, table_name
    )
    assert "Part all_1_1_0 has a broken projection proj1" in check_table_full(
        node, table_name
    )

    break_projection(node, table_name, "proj2", "all_1_1_0", "data")
    reattach(node, table_name)

    assert "all_1_1_0\tproj2\tFILE_DOESNT_EXIST" in get_broken_projections_info(
        node, table_name
    )
    assert "Part all_1_1_0 has a broken projection proj2" in check_table_full(
        node, table_name
    )

    materialize_projection(node, table_name, "proj1")

    assert "has a broken projection" not in check_table_full(node, table_name)


def test_broken_ignored_replicated(cluster):
    node = cluster.instances["node"]

    table_name = "test3"
    table_name2 = "test3_replica"
    create_table(node, table_name, 1)

    insert(node, table_name, 0, 5)
    insert(node, table_name, 5, 5)
    insert(node, table_name, 10, 5)
    insert(node, table_name, 15, 5)

    check(node, table_name, 1)

    create_table(node, table_name2, 2, table_name)
    check(node, table_name2, 1)

    break_projection(node, table_name, "proj1", "all_0_0_0", "data")
    assert "Part all_0_0_0 has a broken projection proj1" in check_table_full(
        node, table_name
    )

    break_part(node, table_name, "all_0_0_0")
    node.query(f"SYSTEM SYNC REPLICA {table_name}")
    assert "has a broken projection" not in check_table_full(node, table_name)


def get_random_string(string_length=8):
    alphabet = string.ascii_letters + string.digits
    return "".join((random.choice(alphabet) for _ in range(string_length)))


def test_broken_projections_in_backups_1(cluster):
    node = cluster.instances["node"]

    table_name = "test4"
    create_table(node, table_name, 1, aggressive_merge=False, data_prefix=table_name)

    node.query("SYSTEM STOP MERGES")

    insert(node, table_name, 0, 5)
    insert(node, table_name, 5, 5)
    insert(node, table_name, 10, 5)
    insert(node, table_name, 15, 5)

    assert ["all_0_0_0", "all_1_1_0", "all_2_2_0", "all_3_3_0"] == get_parts(
        node, table_name
    )

    check(node, table_name, 1)

    break_projection(node, table_name, "proj1", "all_2_2_0", "data")
    check(node, table_name, 0, "proj1", "FILE_DOESNT_EXIST")

    assert "all_2_2_0\tproj1\tNO_FILE_IN_DATA_PART" in get_broken_projections_info(
        node, table_name
    )

    backup_name = f"b1-{get_random_string()}"
    assert "BACKUP_CREATED" in node.query(
        f"""
    set backup_restore_keeper_fault_injection_probability=0.0;
    backup table {table_name} to Disk('backups', '{backup_name}') settings check_projection_parts=false;
    """
    )

    assert "RESTORED" in node.query(
        f"""
    drop table {table_name} sync;
    set backup_restore_keeper_fault_injection_probability=0.0;
    restore table {table_name} from Disk('backups', '{backup_name}');
    """
    )

    node.query("SYSTEM STOP MERGES")

    check(node, table_name, 1)
    assert "" == get_broken_projections_info(node, table_name)


def test_broken_projections_in_backups_2(cluster):
    node = cluster.instances["node"]

    table_name = "test5"
    create_table(node, table_name, 1, aggressive_merge=False, data_prefix=table_name)

    insert(node, table_name, 0, 5)
    insert(node, table_name, 5, 5)
    insert(node, table_name, 10, 5)
    insert(node, table_name, 15, 5)

    assert ["all_0_0_0", "all_1_1_0", "all_2_2_0", "all_3_3_0"] == get_parts(
        node, table_name
    )

    check(node, table_name, 1)
    break_projection(node, table_name, "proj2", "all_2_2_0", "part")
    check(node, table_name, 0, "proj2", "ErrnoException")

    assert "all_2_2_0\tproj2\tFILE_DOESNT_EXIST" == get_broken_projections_info(
        node, table_name
    )

    assert "FILE_DOESNT_EXIST" in node.query_and_get_error(
        f"""
    set backup_restore_keeper_fault_injection_probability=0.0;
    backup table {table_name} to Disk('backups', 'b2')
    """
    )

    materialize_projection(node, table_name, "proj2")
    check(node, table_name, 1)

    backup_name = f"b3-{get_random_string()}"
    assert "BACKUP_CREATED" in node.query(
        f"""
    set backup_restore_keeper_fault_injection_probability=0.0;
    backup table {table_name} to Disk('backups', '{backup_name}') settings check_projection_parts=false;
    """
    )

    assert "RESTORED" in node.query(
        f"""
    drop table {table_name} sync;
    set backup_restore_keeper_fault_injection_probability=0.0;
    restore table {table_name} from Disk('backups', '{backup_name}');
    """
    )
    check(node, table_name, 1)


def test_broken_projections_in_backups_3(cluster):
    node = cluster.instances["node"]

    table_name = "test6"
    create_table(node, table_name, 1, aggressive_merge=False, data_prefix=table_name)

    node.query("SYSTEM STOP MERGES")

    insert(node, table_name, 0, 5)
    insert(node, table_name, 5, 5)
    insert(node, table_name, 10, 5)
    insert(node, table_name, 15, 5)

    assert ["all_0_0_0", "all_1_1_0", "all_2_2_0", "all_3_3_0"] == get_parts(
        node, table_name
    )

    check(node, table_name, 1)

    break_projection(node, table_name, "proj1", "all_1_1_0", "part")
    assert "Part all_1_1_0 has a broken projection proj1" in check_table_full(
        node, table_name
    )
    assert "all_1_1_0\tproj1\tFILE_DOESNT_EXIST" == get_broken_projections_info(
        node, table_name
    )

    backup_name = f"b4-{get_random_string()}"
    assert "BACKUP_CREATED" in node.query(
        f"""
    set backup_restore_keeper_fault_injection_probability=0.0;
    backup table {table_name} to Disk('backups', '{backup_name}') settings check_projection_parts=false, allow_backup_broken_projections=true;
    """
    )

    assert "RESTORED" in node.query(
        f"""
    drop table {table_name} sync;
    set backup_restore_keeper_fault_injection_probability=0.0;
    restore table {table_name} from Disk('backups', '{backup_name}');
    """
    )

    check(node, table_name, 0)
    assert "all_1_1_0\tproj1\tNO_FILE_IN_DATA_PART" == get_broken_projections_info(
        node, table_name
    )


def test_check_part_thread(cluster):
    node = cluster.instances["node"]

    table_name = "check_part_thread_test1"
    create_table(node, table_name, 1)

    insert(node, table_name, 0, 5)
    insert(node, table_name, 5, 5)
    insert(node, table_name, 10, 5)
    insert(node, table_name, 15, 5)

    assert ["all_0_0_0", "all_1_1_0", "all_2_2_0", "all_3_3_0"] == get_parts(
        node, table_name
    )

    # Break data file of projection 'proj2' for part all_2_2_0
    break_projection(node, table_name, "proj2", "all_2_2_0", "data")

    # It will not yet appear in broken projections info.
    assert "proj2" not in get_broken_projections_info(node, table_name)

    # Select now fails with error "File doesn't exist"
    check(node, table_name, 0, "proj2", "FILE_DOESNT_EXIST", do_check_command=False)

    good = False
    for _ in range(10):
        # We marked projection as broken, checkPartThread must not complain about the part.
        good = node.contains_in_log(
            f"{table_name} (ReplicatedMergeTreePartCheckThread): Part all_2_2_0 looks good"
        )
        if good:
            break
        time.sleep(1)

    assert good


def test_broken_on_start(cluster):
    node = cluster.instances["node"]

    table_name = "test1"
    create_table(node, table_name, 1)

    insert(node, table_name, 0, 5)
    insert(node, table_name, 5, 5)
    insert(node, table_name, 10, 5)
    insert(node, table_name, 15, 5)

    assert ["all_0_0_0", "all_1_1_0", "all_2_2_0", "all_3_3_0"] == get_parts(
        node, table_name
    )

    # Break data file of projection 'proj2' for part all_2_2_0
    break_projection(node, table_name, "proj2", "all_2_2_0", "data")

    # It will not yet appear in broken projections info.
    assert "proj2" not in get_broken_projections_info(node, table_name)

    # Select now fails with error "File doesn't exist"
    # We will mark projection as broken.
    check(node, table_name, 0, "proj2", "FILE_DOESNT_EXIST")

    # Projection 'proj2' from part all_2_2_0 will now appear in broken parts info.
    assert "all_2_2_0\tproj2\tNO_FILE_IN_DATA_PART" in get_broken_projections_info(
        node, table_name
    )

    # Second select works, because projection is now marked as broken.
    check(node, table_name, 0)

    node.restart_clickhouse()

    # It will not yet appear in broken projections info.
    assert "proj2" in get_broken_projections_info(node, table_name)

    # Select works
    check(node, table_name, 0)


def test_mutation_with_broken_projection(cluster):
    node = cluster.instances["node"]

    table_name = "test1"
    create_table(node, table_name, 1)

    insert(node, table_name, 0, 5)
    insert(node, table_name, 5, 5)
    insert(node, table_name, 10, 5)
    insert(node, table_name, 15, 5)

    assert ["all_0_0_0", "all_1_1_0", "all_2_2_0", "all_3_3_0"] == get_parts(
        node, table_name
    )

    check(node, table_name, 1)

    node.query(
        f"ALTER TABLE {table_name} DELETE WHERE c == 11 SETTINGS mutations_sync = 1"
    )

    assert ["all_0_0_0_4", "all_1_1_0_4", "all_2_2_0_4", "all_3_3_0_4"] == get_parts(
        node, table_name
    )

    assert "" == get_broken_projections_info(node, table_name)

    check(node, table_name, 1)

    # Break data file of projection 'proj2' for part all_2_2_0_4
    break_projection(node, table_name, "proj2", "all_2_2_0_4", "data")

    # It will not yet appear in broken projections info.
    assert "proj2" not in get_broken_projections_info(node, table_name)

    # Select now fails with error "File doesn't exist"
    # We will mark projection as broken.
    check(node, table_name, 0, "proj2", "FILE_DOESNT_EXIST")

    # Projection 'proj2' from part all_2_2_0_4 will now appear in broken parts info.
    assert "all_2_2_0_4\tproj2\tNO_FILE_IN_DATA_PART" in get_broken_projections_info(
        node, table_name
    )

    # Second select works, because projection is now marked as broken.
    check(node, table_name, 0)

    assert "all_2_2_0_4" in get_broken_projections_info(node, table_name)

    node.query(
        f"ALTER TABLE {table_name} DELETE WHERE _part == 'all_0_0_0_4' SETTINGS mutations_sync = 1"
    )

    # All parts changes because this is how alter delete works,
    # but all parts apart from the first have only hardlinks to files in previous part.
    assert ["all_0_0_0_5", "all_1_1_0_5", "all_2_2_0_5", "all_3_3_0_5"] == get_parts(
        node, table_name
    ) or ["all_1_1_0_5", "all_2_2_0_5", "all_3_3_0_5"] == get_parts(node, table_name)

    # Still broken because it was hardlinked.
    broken = get_broken_projections_info(node, table_name)
    assert (
        "all_2_2_0_5" in broken or "" == broken
    )  # second could be because of a merge.

    if "" == broken:
        check(node, table_name, 1)
    else:
        check(node, table_name, 0)

    node.query(
        f"ALTER TABLE {table_name} DELETE WHERE c == 13 SETTINGS mutations_sync = 1"
    )

    assert ["all_1_1_0_6", "all_2_2_0_6", "all_3_3_0_6"] == get_parts(
        node, table_name
    ) or ["all_0_0_0_6", "all_1_1_0_6", "all_2_2_0_6", "all_3_3_0_6"] == get_parts(
        node, table_name
    )

    # Not broken anymore.
    assert "" == get_broken_projections_info(node, table_name)

    check(node, table_name, 1)
