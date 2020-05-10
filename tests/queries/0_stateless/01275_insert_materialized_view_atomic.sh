#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

# just in case
set -o pipefail

function execute()
{
    ${CLICKHOUSE_CLIENT} -n "$@"
}

#
# TEST SETTINGS
#
TEST_01275_PARTS=9
TEST_01275_MEMORY=$((100<<20))

function cleanup()
{
    for i in $(seq 1 $TEST_01275_PARTS); do
        echo "drop table if exists part_01275_$i;"
        echo "drop table if exists mv_01275_$i;"
    done | execute
    echo 'drop table if exists data_01275;' | execute
    echo 'drop table if exists out_01275;' | execute
}

cleanup
trap cleanup EXIT

#
# CREATE
#
{
cat <<EOL
create table data_01275 (
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

for i in $(seq 1 $TEST_01275_PARTS); do
    echo "create table part_01275_$i as data_01275 Engine=TinyLog();"
    echo "create materialized view mv_01275_$i to part_01275_$i as select * from data_01275 where key%$TEST_01275_PARTS+1 == $i;"
done | execute

echo "create table out_01275 as data_01275 Engine=Merge(currentDatabase(), 'part_01275_');" | execute

#
# INSERT insert_materialized_view_atomic=1 (default)
#
{
cat <<EOL
insert into data_01275 select
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
    # will trigger error on 7'th MV (alphabetic order)
    execute --testmode --max_memory_usage=$TEST_01275_MEMORY
}
# check atomicity
echo 'select count() from out_01275' | execute

#
# INSERT insert_materialized_view_atomic=0
#
{
cat <<EOL
insert into data_01275 select
    number,
    reinterpretAsString(number), // s1
    reinterpretAsString(number), // s2
    reinterpretAsString(number), // s3
    reinterpretAsString(number), // s4
    reinterpretAsString(number), // s5
    reinterpretAsString(number), // s6
    reinterpretAsString(number), // s7
    reinterpretAsString(number)  // s8
from numbers(100000);
EOL
} | {
    execute --max_memory_usage=$TEST_01275_MEMORY --insert_materialized_view_atomic=0
}
echo 'select count() from out_01275' | execute

#
# INSERT insert_materialized_view_atomic=0 with multiple blocks
#
{
cat <<EOL
insert into data_01275 select
    number,
    reinterpretAsString(number), // s1
    reinterpretAsString(number), // s2
    reinterpretAsString(number), // s3
    reinterpretAsString(number), // s4
    reinterpretAsString(number), // s5
    reinterpretAsString(number), // s6
    reinterpretAsString(number), // s7
    reinterpretAsString(number)  // s8
from numbers(100000);
EOL
} | {
    execute --max_memory_usage=$TEST_01275_MEMORY --insert_materialized_view_atomic=0 --min_insert_block_size_rows=50000
}
echo 'select count() from out_01275' | execute
