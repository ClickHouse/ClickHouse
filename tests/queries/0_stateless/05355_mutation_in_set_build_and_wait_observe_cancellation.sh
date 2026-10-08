#!/usr/bin/env bash

# A mutation part task that builds the set of an `IN (subquery)`, or waits for the set another part task
# builds, must observe its own cancellation even when the subquery does a lot of work without reading
# data (here a cross join), so `KILL MUTATION` and `REPLACE PARTITION` do not wait for the set.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# 10M rows with a one second sleep per block: minutes of work, none of it reading from a table.
subquery="SELECT a.number + b.number + sleep(1) FROM numbers(2000) AS a, numbers(5000) AS b"

function wait_merges_gone()
{
    local table=$1
    local deadline=$((SECONDS + 30))
    while [ $SECONDS -lt $deadline ]; do
        [ "$($CLICKHOUSE_CLIENT --query "
            SELECT count() FROM system.merges WHERE database = currentDatabase() AND table = '$table'")" = 0 ] && return
        sleep 0.1
    done
}

# Part A: the main mutating pipeline builds the set itself (`WHERE 1` needs no affected-rows check).
$CLICKHOUSE_CLIENT --query "
CREATE TABLE t_main (id UInt64, v UInt8) ENGINE = MergeTree ORDER BY id
SETTINGS number_of_free_entries_in_pool_to_execute_mutation = 0"
$CLICKHOUSE_CLIENT --query "INSERT INTO t_main VALUES (0, 0), (1, 0), (2, 0)"
$CLICKHOUSE_CLIENT --query "
ALTER TABLE t_main UPDATE v = (id IN ($subquery)) WHERE 1 SETTINGS mutations_sync = 0"

started=0
deadline=$((SECONDS + 60))
while [ $SECONDS -lt $deadline ]; do
    if [ "$($CLICKHOUSE_CLIENT --query "
        SELECT count() FROM system.merges WHERE database = currentDatabase() AND table = 't_main' AND elapsed > 2")" != 0 ]; then
        started=1
        break
    fi
    sleep 0.1
done
[ "$started" = 1 ] || { echo "Part A: the mutation did not start" >&2; exit 1; }

$CLICKHOUSE_CLIENT --query "KILL MUTATION WHERE database = currentDatabase() AND table = 't_main' FORMAT Null"
wait_merges_gone t_main
$CLICKHOUSE_CLIENT --query "SELECT count() FROM system.merges WHERE database = currentDatabase() AND table = 't_main'"
$CLICKHOUSE_CLIENT --query "SELECT sum(v) FROM t_main"

# Part B: three partitions share one set. `ignore` keeps the set out of the index analysis, so every part's
# affected-rows check query needs it: one builds it, the other two wait for it.
$CLICKHOUSE_CLIENT --query "
CREATE TABLE t_wait (id UInt64, p UInt8) ENGINE = MergeTree PARTITION BY p ORDER BY id
SETTINGS number_of_free_entries_in_pool_to_execute_mutation = 0"
$CLICKHOUSE_CLIENT --query "INSERT INTO t_wait VALUES (0, 0), (1, 0)"
$CLICKHOUSE_CLIENT --query "INSERT INTO t_wait VALUES (10, 1), (11, 1)"
$CLICKHOUSE_CLIENT --query "INSERT INTO t_wait VALUES (20, 2), (21, 2)"
$CLICKHOUSE_CLIENT --query "CREATE TABLE t_wait_src AS t_wait"
$CLICKHOUSE_CLIENT --query "INSERT INTO t_wait_src VALUES (100, 0), (110, 1), (120, 2)"
table_uuid=$($CLICKHOUSE_CLIENT --query "
    SELECT uuid FROM system.tables WHERE database = currentDatabase() AND name = 't_wait'")
$CLICKHOUSE_CLIENT --query "
ALTER TABLE t_wait DELETE WHERE ignore(id IN ($subquery)) SETTINGS mutations_sync = 0"

# The partition of each part task, from its query id `<table uuid>::<part name>_<mutation version>`.
roles_query="
    SELECT splitByChar('_', splitByString('::', query_id)[2])[1]
    FROM system.text_log
    WHERE event_date >= yesterday() AND logger_name = 'CreatingSetsTransform'
      AND query_id LIKE concat('${table_uuid}', '::%') AND message LIKE"

builder=""
waiters=""
deadline=$((SECONDS + 120))
while [ $SECONDS -lt $deadline ]; do
    $CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS text_log"
    builder=$($CLICKHOUSE_CLIENT --max_rows_to_read 0 --query "$roles_query 'Building set, key: %' GROUP BY 1")
    waiters=$($CLICKHOUSE_CLIENT --max_rows_to_read 0 --query "$roles_query 'Waiting for set to be built by another thread, key: %' GROUP BY 1 ORDER BY 1")
    [ "$(echo "$builder" | grep -c .)" = 1 ] && [ "$(echo "$waiters" | grep -c .)" = 2 ] && break
    sleep 1
done
if [ "$(echo "$builder" | grep -c .)" != 1 ] || [ "$(echo "$waiters" | grep -c .)" != 2 ]; then
    echo "Part B: expected one builder and two waiters, got builder [$builder], waiters [$waiters]" >&2
    exit 1
fi
waiter_1=$(echo "$waiters" | head -n 1)
waiter_2=$(echo "$waiters" | tail -n 1)

# Only the first waiter is cancelled, while the set is still being built.
$CLICKHOUSE_CLIENT --query "ALTER TABLE t_wait REPLACE PARTITION ID '$waiter_1' FROM t_wait_src"

# The builder is cancelled. It abandons the set instead of failing it, so the other waiter builds it itself.
$CLICKHOUSE_CLIENT --query "ALTER TABLE t_wait REPLACE PARTITION ID '$builder' FROM t_wait_src"
retried=0
deadline=$((SECONDS + 60))
while [ $SECONDS -lt $deadline ]; do
    $CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS text_log"
    retried=$($CLICKHOUSE_CLIENT --max_rows_to_read 0 --query "
        SELECT count() > 0 FROM system.text_log
        WHERE event_date >= yesterday() AND logger_name = 'CreatingSetsTransform'
          AND query_id LIKE concat('${table_uuid}', '::${waiter_2}\\_%') AND message LIKE 'Failed to use set from cache, key: %'")
    [ "$retried" = 1 ] && break
    sleep 1
done
echo "$retried"

$CLICKHOUSE_CLIENT --query "KILL MUTATION WHERE database = currentDatabase() AND table = 't_wait' FORMAT Null"
wait_merges_gone t_wait
$CLICKHOUSE_CLIENT --query "SELECT count() FROM system.merges WHERE database = currentDatabase() AND table = 't_wait'"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM t_wait"

$CLICKHOUSE_CLIENT --query "DROP TABLE t_main"
$CLICKHOUSE_CLIENT --query "DROP TABLE t_wait"
$CLICKHOUSE_CLIENT --query "DROP TABLE t_wait_src"
