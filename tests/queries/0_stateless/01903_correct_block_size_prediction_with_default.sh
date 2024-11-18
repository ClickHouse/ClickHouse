#!/usr/bin/env bash
# Tags: no-random-merge-tree-settings, no-random-settings

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

sql="toUInt16OrNull(arrayFirst((v, k) -> (k = '4Id'), arr[2], arr[1]))"

# Create the table and fill it
$CLICKHOUSE_CLIENT --query="
    CREATE TABLE test_extract(str String,  arr Array(Array(String)) ALIAS extractAllGroupsHorizontal(str, '\\W(\\w+)=(\"[^\"]*?\"|[^\",}]*)')) ENGINE=MergeTree() PARTITION BY tuple() ORDER BY tuple();
    INSERT INTO test_extract (str) WITH range(8) as range_arr, arrayMap(x-> concat(toString(x),'Id'), range_arr) as key, arrayMap(x -> rand() % 8, range_arr) as val, arrayStringConcat(arrayMap((x,y) -> concat(x,'=',toString(y)), key, val),',') as str SELECT str FROM numbers(500000);
    ALTER TABLE test_extract ADD COLUMN 15Id Nullable(UInt16) DEFAULT $sql;"

function test()
{
    # Execute two queries and compare if they have similar memory usage:
    # The first query uses the default column value, while the second explicitly uses the same SQL as the default value.
    # Follow https://github.com/ClickHouse/ClickHouse/issues/17317 for more info about the issue
    where=$1

    uuid_1=$(cat /proc/sys/kernel/random/uuid)
    $CLICKHOUSE_CLIENT --query="SELECT uniq(15Id) FROM test_extract $where SETTINGS max_threads=1" --query_id=$uuid_1
    uuid_2=$(cat /proc/sys/kernel/random/uuid)
    $CLICKHOUSE_CLIENT --query="SELECT uniq($sql) FROM test_extract $where SETTINGS max_threads=1" --query_id=$uuid_2
    $CLICKHOUSE_CLIENT --query="
        SYSTEM FLUSH LOGS;
        WITH memory_1 AS (SELECT memory_usage FROM system.query_log WHERE current_database = currentDatabase() AND query_id='$uuid_1' AND type = 'QueryFinish' as memory_1),
             memory_2 AS (SELECT memory_usage FROM system.query_log WHERE current_database = currentDatabase() AND query_id='$uuid_2' AND type = 'QueryFinish' as memory_2)
                SELECT memory_1.memory_usage <= 1.2 * memory_2.memory_usage OR
                       memory_2.memory_usage <= 1.2 * memory_1.memory_usage FROM memory_1, memory_2;"
}

test ""
test "PREWHERE 15Id < 4"
test "WHERE 15Id < 4"
