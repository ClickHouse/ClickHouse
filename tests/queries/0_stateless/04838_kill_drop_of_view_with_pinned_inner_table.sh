#!/usr/bin/env bash
# Tags: long

# `DROP TABLE ... SYNC` of a materialized view waits for its inner table to be finally dropped.
# While another query holds a reference to the inner table, that wait cannot finish, so it must
# be interruptible by `KILL QUERY`. The test checks that the killed `DROP` returns while the
# reference holder is still running.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS mv_04838;
    DROP TABLE IF EXISTS src_04838;
    CREATE TABLE src_04838 (x UInt64) ENGINE = MergeTree ORDER BY x;
    CREATE MATERIALIZED VIEW mv_04838 ENGINE = MergeTree ORDER BY x AS SELECT x FROM src_04838;
    INSERT INTO src_04838 SELECT number FROM numbers(600);
"

inner_table=$($CLICKHOUSE_CLIENT -q "SELECT name FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner_id.%' LIMIT 1")

holder_query_id="04838_holder_${CLICKHOUSE_DATABASE}"
drop_query_id="04838_drop_${CLICKHOUSE_DATABASE}"

# Holds a reference to the inner table for up to ~3 minutes (killed right after the check below).
$CLICKHOUSE_CLIENT --query_id "$holder_query_id" -q "SELECT sum(sleepEachRow(0.3)) FROM \`$inner_table\` SETTINGS max_block_size = 1, max_threads = 1, function_sleep_max_microseconds_per_block = 300000000, enable_parallel_replicas = 0 FORMAT Null" >/dev/null 2>&1 &
holder_pid=$!

for _ in {1..300}
do
    started=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.processes WHERE query_id = '$holder_query_id'")
    [[ $started == 1 ]] && break
    sleep 0.1
done

$CLICKHOUSE_CLIENT --query_id "$drop_query_id" -q "DROP TABLE mv_04838 SYNC" >/dev/null 2>&1 &
drop_pid=$!

# Wait until the DROP marks the inner table as dropped: right after that it starts waiting
# for the inner table to be finally dropped, which cannot happen while the holder runs.
for _ in {1..300}
do
    marked=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.dropped_tables WHERE database = currentDatabase() AND table = '$inner_table'")
    [[ $marked -ge 1 ]] && break
    sleep 0.1
done

$CLICKHOUSE_CLIENT -q "KILL QUERY WHERE query_id = '$drop_query_id' ASYNC FORMAT Null"

wait $drop_pid

holder_still_running=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.processes WHERE query_id = '$holder_query_id'")
if [[ $holder_still_running == 1 ]]
then
    echo "drop returned while holder was still running"
else
    echo "drop returned only after holder finished"
fi

$CLICKHOUSE_CLIENT -q "KILL QUERY WHERE query_id = '$holder_query_id' ASYNC FORMAT Null"
wait $holder_pid

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS mv_04838 SYNC"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS src_04838"
