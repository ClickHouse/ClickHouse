#!/usr/bin/env bash

#
# Check that KILL MUTATION can be executed in parallel for different tables.
# For this two identical tables will be created:
# - on one table ALTER + KILL MUTATION will be executed
# - on another table only ALTER, that should be succeed
#

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -nm -q "
    drop table if exists data_01900_1;
    drop table if exists data_01900_2;

    create table data_01900_1 (k UInt64, s String) engine=MergeTree() order by k;
    create table data_01900_2 (k UInt64, s String) engine=MergeTree() order by k;

    insert into data_01900_1 values (1, 'hello'), (2, 'world');
    insert into data_01900_2 values (1, 'hello'), (2, 'world');
"

# default finished_mutations_to_keep is 100
# so 100 mutations will be scheduled and killed later.
for i in {1..100}; do
    echo "alter table data_01900_1 update s = 'foo_$i' where 1;"
done | $CLICKHOUSE_CLIENT -nm

# but these mutations should not be killed.
(
    for i in {1..100}; do
        echo "alter table data_01900_2 update s = 'bar_$i' where 1;"
    done | $CLICKHOUSE_CLIENT -nm --mutations_sync=1
) &
$CLICKHOUSE_CLIENT --format Null -nm -q "kill mutation where table = 'data_01900_1' and database = '$CLICKHOUSE_DATABASE';"
wait

$CLICKHOUSE_CLIENT -nm -q "select * from data_01900_2"

$CLICKHOUSE_CLIENT -q "drop table data_01900_1"
$CLICKHOUSE_CLIENT -q "drop table data_01900_2"
