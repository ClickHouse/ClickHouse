#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# just in case
set -o pipefail

function execute()
{
    ${CLICKHOUSE_CLIENT} -n "$@"
}

#
# TEST SETTINGS
#
TEST_01278_PARTS=9
TEST_01278_MEMORY=$((100<<20))

function cleanup()
{
    for i in $(seq 1 $TEST_01278_PARTS); do
        echo "drop table if exists part_01278_$i;"
        echo "drop table if exists mv_01278_$i;"
    done | execute
    echo 'drop table if exists data_01278;' | execute
    echo 'drop table if exists out_01278;' | execute
    echo 'drop table if exists null_01278;' | execute
}

cleanup
trap cleanup EXIT

#
# CREATE
#
{
cat <<EOL
create table data_01278 (
    key UInt64,
    // create bunch of fields to increase memory usage for the query
    s1 Nullable(String),
    s2 Nullable(String),
    s3 Nullable(String),
    s4 Nullable(String),
    s5 Nullable(String),
    s6 Nullable(String),
    s7 Nullable(String),
    s8 Nullable(String)
) Engine=Null()
EOL
} | execute

echo "create table null_01278 as data_01278 Engine=Null();" | execute
for i in $(seq 1 $TEST_01278_PARTS); do
    echo "create table part_01278_$i as data_01278 Engine=Buffer('$CLICKHOUSE_DATABASE', null_01278, 1, 86400, 86400, 1e5, 1e6, 10e6, 100e6);"
    echo "create materialized view mv_01278_$i to part_01278_$i as select * from data_01278 where key%$TEST_01278_PARTS+1 != $i;"
done | execute
echo "create table out_01278 as data_01278 Engine=Merge('$CLICKHOUSE_DATABASE', 'part_01278_');" | execute

#
# INSERT
#
function execute_insert()
{
    {
    cat <<EOL
insert into data_01278 select
    number,
    reinterpretAsString(number), // s1
    reinterpretAsString(number), // s2
    reinterpretAsString(number), // s3
    reinterpretAsString(number), // s4
    reinterpretAsString(number), // s5
    reinterpretAsString(number), // s6
    reinterpretAsString(number), // s7
    reinterpretAsString(number)  // s8
from numbers(100000); -- { serverError 241; }
EOL
    } | {
        execute --max_memory_usage=$TEST_01278_MEMORY --optimize_trivial_insert_select='false' "$@"
    }
    echo 'select count() from out_01278' | execute
}

# fails
execute_insert --testmode
execute_insert --testmode --min_insert_block_size_rows=1 --min_insert_block_size_rows_for_materialized_views=$((1<<20))

# passes
execute_insert --min_insert_block_size_rows=1
execute_insert --min_insert_block_size_rows_for_materialized_views=1
