#!/usr/bin/env bash
# shellcheck disable=SC2046

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -nm -q "
DROP TABLE IF EXISTS test SYNC;
CREATE TABLE test
(
    a String,
    b String,
    c Int32,
    d Int32,
    e Int32,

    PROJECTION proj
    (
        SELECT c ORDER BY d
    ),
    PROJECTION proj_2
    (
        SELECT d ORDER BY c
    )
)
ENGINE = ReplicatedMergeTree('/test4/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/', '1') PRIMARY KEY (a)
SETTINGS min_bytes_for_wide_part = 0,
    max_parts_to_merge_at_once=3,
    enable_vertical_merge_algorithm=1,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 1;
"

table_uuid=$($CLICKHOUSE_CLIENT -q "SELECT uuid FROM system.tables WHERE table='test' and database=currentDatabase()")

function random()
{
    cat /dev/urandom | LC_ALL=C tr -dc 'a-zA-Z' | fold -w ${1:-8} | head -n 1
}

function insert()
{
    offset=$1
    size=$2
    echo 'insert new part'
    $CLICKHOUSE_CLIENT -q "INSERT INTO test SELECT number, number, number, number, number%2 FROM numbers($offset, $size);"
}

function break_projection()
{
    part_name=$1
    parent_name=$2
    break_type=$3

    read -r disk_name part_path <<< $($CLICKHOUSE_CLIENT -nm -q "
    SELECT disk_name, path
    FROM system.projection_parts
    WHERE table='test'
    AND database=currentDatabase()
    AND active=1
    AND part_name='$part_name'
    AND parent_name='$parent_name'
    LIMIT 1;
    ")

    if [ "$break_type" = "data" ]
        then
           rm "$part_path/d.bin"
           rm "$part_path/c.bin"
           echo "broke data of part '$part_name' (parent part: $parent_name)"
        else
           rm "$part_path/columns.txt"
           echo "broke metadata of part '$part_name' (parent part: $parent_name)"
    fi
}

function broken_projections_info()
{
    echo 'broken projections info'
    $CLICKHOUSE_CLIENT -q "
    SELECT parent_name, name, errors.name FROM
    (
        SELECT parent_name, name, exception_code
        FROM system.projection_parts
        WHERE table='test'
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
    expect_broken_part=""
    expected_error=""
    if [ $# -ne 0 ]; then
        expect_broken_part=$1
        expected_error=$2
    fi

    echo 'system.parts'
    $CLICKHOUSE_CLIENT -q "
    SELECT name, active, projections
    FROM system.parts
    WHERE table='test' AND database=currentDatabase()
    ORDER BY name;"

    echo "select from projection 'proj', expect error: $expect_broken_part"
    query_id=$(random 8)

    if [ "$expect_broken_part" = "proj" ]
        then
            $CLICKHOUSE_CLIENT --optimize_use_projections 1 --send_logs_level 'fatal' --query_id $query_id -q "SELECT c FROM test WHERE d == 12 ORDER BY c;" 2>&1 | grep -o $expected_error
        else
            $CLICKHOUSE_CLIENT --optimize_use_projections 1 --query_id $query_id -q "SELECT c FROM test WHERE d == 12 OR d == 16 ORDER BY c;"
            echo 'used projections'
            $CLICKHOUSE_CLIENT -nm -q "
            SYSTEM FLUSH LOGS;
            SELECT query, projections FROM system.query_log WHERE current_database=currentDatabase() AND query_id='$query_id' AND type='QueryFinish'
            "
    fi

    echo "select from projection 'proj_2', expect error: $expect_broken_part"
    query_id=$(random 8)

    if [ "$expect_broken_part" = "proj_2" ]
        then
            $CLICKHOUSE_CLIENT --optimize_use_projections 1 --send_logs_level 'fatal' --query_id $query_id -q "SELECT d FROM test WHERE c == 12 ORDER BY d;" 2>&1 | grep -o $expected_error
        else
            $CLICKHOUSE_CLIENT --optimize_use_projections 1 --query_id $query_id -q "SELECT d FROM test WHERE c == 12 OR c == 16 ORDER BY d;"
            echo 'used projections'
            $CLICKHOUSE_CLIENT -nm -q "
            SYSTEM FLUSH LOGS;
            SELECT query, projections FROM system.query_log WHERE current_database=currentDatabase() AND query_id='$query_id' AND type='QueryFinish'
            "
    fi

    echo 'check table'
    $CLICKHOUSE_CLIENT -nm -q "
    SET send_logs_level='fatal';
    CHECK TABLE test;"
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
    projection=$1
    echo "materialize projection $projection"
    $CLICKHOUSE_CLIENT -q "ALTER TABLE test MATERIALIZE PROJECTION $projection SETTINGS mutations_sync=2"
}

function check_table_full()
{
    echo 'check table full'
    $CLICKHOUSE_CLIENT -nm -q "
    SET send_logs_level='fatal';
    CHECK TABLE test SETTINGS check_query_single_value_result = 0;
" | grep "broken"
}


insert 0 5

insert 5 5

insert 10 5

insert 15 5

check

# Break metadata file of projection 'proj'
break_projection proj all_2_2_0 metadata

# Do select and after "check table" query.
# Select works because it does not read columns.txt.
check

# Projection 'proj' from part all_2_2_0 will now appear in broken parts info
# because it was marked broken during "check table" query.
# TODO: try to mark it during select as well
broken_projections_info

# Check table query will also show a list of parts which have broken projections.
check_table_full

# Break data file of projection 'proj_2' for part all_2_2_0
break_projection proj_2 all_2_2_0 data

# It will not yet appear in broken projections info.
broken_projections_info

# Select now fails with error "File doesn't exist"
check "proj_2" "FILE_DOESNT_EXIST"

# Projection 'proj_2' from part all_2_2_0 will now appear in broken parts info.
broken_projections_info

# Second select works, because projection is now marked as broken.
check

# Break data file of projection 'proj_2' for part all_3_3_0
break_projection proj_2 all_3_3_0 data

# It will not yet appear in broken projections info.
broken_projections_info

insert 20 5

insert 25 5

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
broken_projections_info

check

break_projection proj all_1_1_0 metadata

reattach

broken_projections_info

break_projection proj_2 all_1_1_0 data

reattach

broken_projections_info

check

check_table_full

materialize_projection proj

check_table_full

check

materialize_projection proj_2

check_table_full

break_projection proj all_3_5_1_7 data

insert 30 5

optimize 1 0

insert 35 5

optimize 1 0

check

$CLICKHOUSE_CLIENT -nm -q "
DROP TABLE test SYNC;
"
