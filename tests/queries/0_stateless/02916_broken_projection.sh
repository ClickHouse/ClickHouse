#!/usr/bin/env bash
# Tags: long, no-random-merge-tree-settings, no-random-settings, no-s3-storage
# shellcheck disable=SC2046

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

function create_table()
{
    test_id=$1
    name=$2
    replica=$3
    $CLICKHOUSE_CLIENT -nm -q "
    DROP TABLE IF EXISTS $name SYNC;
    CREATE TABLE $name
    (
        a String,
        b String,
        c Int64,
        d Int64,
        e Int64,

        PROJECTION proj
        (
            SELECT c ORDER BY d
        ),
        PROJECTION proj_2
        (
            SELECT d ORDER BY c
        )
    )
    ENGINE = ReplicatedMergeTree('/test_broken_projection_32_$test_id/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/', '$replica') ORDER BY a
    SETTINGS min_bytes_for_wide_part = 0,
        max_parts_to_merge_at_once=3,
        enable_vertical_merge_algorithm=1,
        vertical_merge_algorithm_min_rows_to_activate = 1,
        vertical_merge_algorithm_min_columns_to_activate = 1,
        vertical_merge_algorithm_min_columns_to_activate = 1,
        compress_primary_key=0;
    "
}

function random()
{
    cat /dev/urandom | LC_ALL=C tr -dc 'a-zA-Z' | fold -w ${1:-8} | head -n 1
}

function insert()
{
    table=$1
    offset=$2
    size=$3
    echo 'insert new part'
    $CLICKHOUSE_CLIENT -q "INSERT INTO $table SELECT number, number, number, number, number%2 FROM numbers($offset, $size) SETTINGS insert_keeper_fault_injection_probability=0.0;"
}

function break_projection()
{
    table=$1
    part_name=$2
    parent_name=$3
    break_type=$4

    read -r part_path <<< $($CLICKHOUSE_CLIENT -nm -q "
    SELECT path
    FROM system.projection_parts
    WHERE table='$table'
    AND database=currentDatabase()
    AND active=1
    AND part_name='$part_name'
    AND parent_name='$parent_name'
    ORDER BY modification_time DESC
    LIMIT 1;
    ")

    $CLICKHOUSE_CLIENT -q "select throwIf(substring('$part_path', 1, 1) != '/', 'Path is relative: $part_path')" || exit

    if [ "$break_type" = "data" ]
        then
           rm "$part_path/d.bin"
           rm "$part_path/c.bin"
           echo "broke data of part '$part_name' (parent part: $parent_name)"
    fi
    if [ "$break_type" = "metadata" ]
        then
           rm "$part_path/columns.txt"
           echo "broke metadata of part '$part_name' (parent part: $parent_name)"
    fi
    if [ "$break_type" = "part" ]
        then
           rm -r "$part_path"
           echo "broke all data of part '$part_name' (parent part: $parent_name)"
    fi
}

function break_part()
{
    table=$1
    part_name=$2

    read -r part_path <<< $($CLICKHOUSE_CLIENT -nm -q "
    SELECT path
    FROM system.parts
    WHERE table='$table'
    AND database=currentDatabase()
    AND active=1
    AND part_name='$part_name'
    ORDER BY modification_time DESC
    LIMIT 1;
    ")

    if [ "$part_path" = "" ]
       then
           echo "Part path is empty"
           exit
    fi

    rm $part_path/columns.txt
    echo "broke data of part '$part_name'"
}

function broken_projections_info()
{
    table=$1
    echo 'broken projections info'
    $CLICKHOUSE_CLIENT -q "
    SELECT parent_name, name, errors.name FROM
    (
        SELECT parent_name, name, exception_code
        FROM system.projection_parts
        WHERE table='$table'
        AND database=currentDatabase()
        AND is_broken = 1
    ) AS parts_info
    INNER JOIN system.errors AS errors
    ON parts_info.exception_code = errors.code
    ORDER BY parent_name, name
"
}

function check()
{
    table=$1
    expect_broken_part=""
    expected_error=""
    if [ $# -gt 1 ]; then
        expect_broken_part=$2
        expected_error=$3
    fi

    #echo 'system.parts'
    #$CLICKHOUSE_CLIENT -q "
    #SELECT name, active, projections
    #FROM system.parts
    #WHERE table='$table' AND database=currentDatabase()
    #ORDER BY name;"

    query_id=$(random 8)

    if [ "$expect_broken_part" = "proj" ]
        then
            echo "select from projection 'proj', expect error: $expect_broken_part"
            $CLICKHOUSE_CLIENT --optimize_use_projections 1 --query_id $query_id -nm -q "
                SET send_logs_level='fatal';
                SELECT c FROM $table WHERE d == 12 ORDER BY c;
            " 2>&1 | grep -oF "$expected_error"
        else
            echo "select from projection 'proj'"
            $CLICKHOUSE_CLIENT --optimize_use_projections 1 --query_id $query_id -q "SELECT c FROM $table WHERE d == 12 OR d == 16 ORDER BY c;"
            echo 'used projections'
            $CLICKHOUSE_CLIENT -nm -q "
                SYSTEM FLUSH LOGS;
                SELECT query, splitByChar('.', arrayJoin(projections))[-1] FROM system.query_log WHERE current_database=currentDatabase() AND query_id='$query_id' AND type='QueryFinish'
            "
    fi

    query_id=$(random 8)

    if [ "$expect_broken_part" = "proj_2" ]
        then
            echo "select from projection 'proj_2', expect error: $expect_broken_part"
            $CLICKHOUSE_CLIENT --optimize_use_projections 1 --query_id $query_id -nm -q "
                SET send_logs_level='fatal';
                SELECT d FROM $table WHERE c == 12 ORDER BY d;
            " 2>&1 | grep -oF "$expected_error"
        else
            echo "select from projection 'proj_2'"
            $CLICKHOUSE_CLIENT --optimize_use_projections 1 --query_id $query_id -q "SELECT d FROM $table WHERE c == 12 OR c == 16 ORDER BY d;"
            echo 'used projections'
            $CLICKHOUSE_CLIENT -nm -q "
                SYSTEM FLUSH LOGS;
                SELECT query, splitByChar('.', arrayJoin(projections))[-1] FROM system.query_log WHERE current_database=currentDatabase() AND query_id='$query_id' AND type='QueryFinish'
            "
    fi

    echo 'check table'
    $CLICKHOUSE_CLIENT -nm -q "
    SET send_logs_level='fatal';
    CHECK TABLE $table;"
}

function optimize()
{
    final=$1
    no_wait=$2

    echo 'optimize'
    query="OPTIMIZE TABLE test"

    if [ $final -eq 1 ]; then
        query="$query FINAL"
    fi
    if [ $no_wait -eq 1 ]; then
        query="$query SETTINGS alter_sync=0"
    fi

    echo $query

    $CLICKHOUSE_CLIENT -q "$query"
}

function reattach()
{
    echo 'Detach - Attach'
    $CLICKHOUSE_CLIENT -nm -q "
    SET send_logs_level='fatal';
    DETACH TABLE test;
    ATTACH TABLE test;
    "
}

function materialize_projection
{
    table=$1
    projection=$2
    echo "materialize projection $projection"
    $CLICKHOUSE_CLIENT -q "ALTER TABLE $table MATERIALIZE PROJECTION $projection SETTINGS mutations_sync=2"
}

function check_table_full()
{
    table=$1
    expect_broken_part=$2
    echo "check table full ($1 - $2)"
    if [ "$expect_broken_part" = "" ]
       then
           $CLICKHOUSE_CLIENT -nm -q "
           SET send_logs_level='fatal';
           CHECK TABLE $table SETTINGS check_query_single_value_result = 0;
           " | grep "broken"
       else
           $CLICKHOUSE_CLIENT -nm -q "
           SET send_logs_level='fatal';
           CHECK TABLE $table SETTINGS check_query_single_value_result = 0;
           " | grep "broken" | grep -o $expect_broken_part | head -n 1
    fi
}

function test1()
{
    create_table test1 test 1

    table_uuid=$($CLICKHOUSE_CLIENT -q "SELECT uuid FROM system.tables WHERE table='test' and database=currentDatabase()")

    insert test 0 5

    insert test 5 5

    insert test 10 5

    insert test 15 5

    check test

    # Break metadata file of projection 'proj'
    break_projection test proj all_2_2_0 metadata

    # Do select and after "check table" query.
    # Select works because it does not read columns.txt.
    check test

    # Projection 'proj' from part all_2_2_0 will now appear in broken parts info
    # because it was marked broken during "check table" query.
    # TODO: try to mark it during select as well
    broken_projections_info test

    # Check table query will also show a list of parts which have broken projections.
    check_table_full test "all_2_2_0"

    # Break data file of projection 'proj_2' for part all_2_2_0
    break_projection test proj_2 all_2_2_0 data

    # It will not yet appear in broken projections info.
    broken_projections_info test

    # Select now fails with error "File doesn't exist"
    check test "proj_2" FILE_DOESNT_EXIST

    # Projection 'proj_2' from part all_2_2_0 will now appear in broken parts info.
    broken_projections_info test

    # Second select works, because projection is now marked as broken.
    check test

    # Break data file of projection 'proj_2' for part all_3_3_0
    break_projection test proj_2 all_3_3_0 data

    # It will not yet appear in broken projections info.
    broken_projections_info test

    insert test 20 5

    insert test 25 5

    # Part all_3_3_0 has 'proj' and 'proj_2' projections, but 'proj_2' is broken and server does NOT know it yet.
    # Parts all_4_4_0 and all_5_5_0 have both non-broken projections.
    # So a merge will be create for future part all_3_5_1.
    # During merge it will fail to read from 'proj_2' of part all_3_3_0 and proj_2 will be marked broken.
    # Merge will be retried and on second attempt it will succeed.
    # The result part all_3_5_1 will have only 1 projection - 'proj', because
    # it will skip 'proj_2' as it will see that one part does not have it anymore in the set of valid projections.
    optimize 0 1
    sleep 2

    $CLICKHOUSE_CLIENT -nm -q "
    SYSTEM FLUSH LOGS;
    SELECT count() FROM system.text_log
    WHERE level='Error'
    AND logger_name='MergeTreeBackgroundExecutor'
    AND message like 'Exception while executing background task {$table_uuid:all_3_5_1}%Cannot open file%proj_2.proj/c.bin%'
    "

    # Projection 'proj_2' from part all_2_2_0 will now appear in broken parts info.
    broken_projections_info test

    check test

    break_projection test proj all_1_1_0 metadata

    reattach

    broken_projections_info test

    break_projection test proj_2 all_1_1_0 data

    reattach

    broken_projections_info test

    check test

    check_table_full test all_1_1_0

    materialize_projection test proj

    check_table_full test

    check test

    materialize_projection test proj_2

    check_table_full test

    break_projection test proj all_3_5_1_7 data

    insert test 30 5

    optimize 1 0

    insert test 35 5

    optimize 1 0

    check test
}

function test2()
{
    create_table test2 test2 1

    insert test2 0 5

    insert test2 5 5

    insert test 10 5

    insert test 15 5

    check test2

    create_table test2 test2_replica 2

    check test2_replica

    break_projection test2 proj all_0_0_0 data

    check_table_full test2 all_0_0_0

    check test2

    break_part test2 all_0_0_0

    check_table_full test2 all_0_0_0

    check test2

    $CLICKHOUSE_CLIENT -q "SYSTEM SYNC REPLICA test2;"

    check test2
}

function test3()
{
    create_table test3 test 1

    insert test 0 5

    insert test 5 5

    insert test 10 5

    insert test 15 5

    check test

    break_projection test proj all_2_2_0 data

    check test proj FILE_DOESNT_EXIST

    broken_projections_info test

    ${CLICKHOUSE_CLIENT} -nm --query "
    set backup_restore_keeper_fault_injection_probability=0.0;
    backup table ${CLICKHOUSE_DATABASE}.test to Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}') settings check_projection_parts=false;
    " | grep -o "BACKUP_CREATED"

    ${CLICKHOUSE_CLIENT} -nm --stacktrace --query "
    drop table test sync;
    set backup_restore_keeper_fault_injection_probability=0.0;
    restore table ${CLICKHOUSE_DATABASE}.test from Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}');
    " | grep -o "RESTORED"

    check test

    broken_projections_info test

    break_projection test proj all_2_2_0 part

    check test proj Errno

    broken_projections_info test

    ${CLICKHOUSE_CLIENT} -nm --query "
    set send_logs_level='fatal';
    set backup_restore_keeper_fault_injection_probability=0.0;
    backup table ${CLICKHOUSE_DATABASE}.test to Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}_2')
    " 2>&1 | grep -o "FILE_DOESNT_EXIST"

    materialize_projection test proj

    check test

    broken_projections_info test

    ${CLICKHOUSE_CLIENT} -nm --query "
    set send_logs_level='fatal';
    set backup_restore_keeper_fault_injection_probability=0.0;
    backup table ${CLICKHOUSE_DATABASE}.test to Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}_3')
    " | grep -o "BACKUP_CREATED"

    ${CLICKHOUSE_CLIENT} -nm --stacktrace --query "
    drop table test sync;
    set send_logs_level='fatal';
    set backup_restore_keeper_fault_injection_probability=0.0;
    restore table ${CLICKHOUSE_DATABASE}.test from Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}_3');
    " | grep -o "RESTORED"

    check test

    break_projection test proj all_1_1_0 part

    check test proj FILE_DOESNT_EXIST

    broken_projections_info test

    ${CLICKHOUSE_CLIENT} -nm --query "
    set send_logs_level='fatal';
    set backup_restore_keeper_fault_injection_probability=0.0;
    backup table ${CLICKHOUSE_DATABASE}.test to Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}_4')
    settings check_projection_parts=false, allow_backup_broken_projections=true;
    " | grep -o "BACKUP_CREATED"

    ${CLICKHOUSE_CLIENT} -nm --stacktrace --query "
    drop table test sync;
    set send_logs_level='fatal';
    set backup_restore_keeper_fault_injection_probability=0.0;
    restore table ${CLICKHOUSE_DATABASE}.test from Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}_4');
    " | grep -o "RESTORED"

    check test

    broken_projections_info test
}

test1
test2
test3


$CLICKHOUSE_CLIENT -nm -q "
DROP TABLE IF EXISTS test SYNC;
DROP TABLE IF EXISTS test2 SYNC;
DROP TABLE IF EXISTS test2_replica SYNC;
"
