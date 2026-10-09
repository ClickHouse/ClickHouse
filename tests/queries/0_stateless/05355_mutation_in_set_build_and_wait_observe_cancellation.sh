#!/usr/bin/env bash

# A mutation part task that builds the set of an `IN (subquery)`, or waits for the set another part task
# builds, must observe its own cancellation even when the subquery does a lot of work without reading
# data or returning rows (here a cross join), so `KILL MUTATION` and `REPLACE PARTITION` do not wait for the set.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# 10M rows with a one second sleep per block: minutes of work, none of it reading from a table.
subquery="SELECT a.number + b.number + sleep(1) FROM numbers(2000) AS a, numbers(5000) AS b"
# The same work, but every row is filtered out, so no block reaches the set.
filtered_subquery="SELECT a.number FROM numbers(2000) AS a, numbers(5000) AS b WHERE a.number + b.number + sleep(1) = 1000000"

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

# Part A: the main mutating pipeline builds the set itself: the affected-rows check of `WHERE 1` does not need it.
$CLICKHOUSE_CLIENT --query "
CREATE TABLE t_main (id UInt64, v UInt8) ENGINE = MergeTree ORDER BY id
SETTINGS number_of_free_entries_in_pool_to_execute_mutation = 0"
$CLICKHOUSE_CLIENT --query "INSERT INTO t_main VALUES (0, 0), (1, 0), (2, 0)"
$CLICKHOUSE_CLIENT --query "
ALTER TABLE t_main UPDATE v = (id IN ($filtered_subquery)) WHERE 1 SETTINGS mutations_sync = 0"

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
# affected-rows check query needs it: one builds it, the others wait for it. The roles are found one at a time,
# so at most two of the part tasks have to run at once in the shared background pool.
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

# The partitions whose part task logged a message, from the query id `<table uuid>::<part name>_<mutation version>`.
function partitions_logging()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS text_log"
    $CLICKHOUSE_CLIENT --max_rows_to_read 0 --query "
        SELECT DISTINCT splitByChar('_', splitByString('::', query_id)[2])[1] AS p
        FROM system.text_log
        WHERE event_date >= yesterday() AND logger_name = 'CreatingSetsTransform'
          AND query_id LIKE concat('${table_uuid}', '::%') AND message LIKE '$1%'
        ORDER BY p"
}

# One deadline for both waiters: the set takes more than 150 seconds to build, so a task that starts before it
# still finds the set in progress.
roles_deadline=$((SECONDS + 120))

builder=""
waiter_1=""
while [ $SECONDS -lt $roles_deadline ]; do
    builder=$(partitions_logging 'Building set, key: ')
    waiter_1=$(partitions_logging 'Waiting for set to be built by another thread, key: ' | sed -n 1p)
    [ "$(echo "$builder" | grep -c .)" = 1 ] && [ -n "$waiter_1" ] && break
    sleep 1
done
if [ "$(echo "$builder" | grep -c .)" != 1 ] || [ -z "$waiter_1" ]; then
    echo "Part B: expected one builder and a waiter, got builder [$builder], waiter [$waiter_1]" >&2
    exit 1
fi

# Only the first waiter is cancelled, while the set is still being built.
$CLICKHOUSE_CLIENT --query "ALTER TABLE t_wait REPLACE PARTITION ID '$waiter_1' FROM t_wait_src"

# The task of the remaining partition waits for the set too; under a busy pool it may start only now.
waiter_2=""
while [ $SECONDS -lt $roles_deadline ]; do
    waiter_2=$(partitions_logging 'Waiting for set to be built by another thread, key: ' \
        | grep -vxF -e "$builder" -e "$waiter_1" | sed -n 1p)
    [ -n "$waiter_2" ] && break
    sleep 1
done
if [ -z "$waiter_2" ]; then
    echo "Part B: the task of the third partition did not wait for the set" >&2
    exit 1
fi

# The builder is cancelled. It abandons the set instead of failing it, so the other waiter builds it itself.
$CLICKHOUSE_CLIENT --query "ALTER TABLE t_wait REPLACE PARTITION ID '$builder' FROM t_wait_src"
retried=0
deadline=$((SECONDS + 60))
while [ $SECONDS -lt $deadline ]; do
    $CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS text_log"
    retried=$($CLICKHOUSE_CLIENT --max_rows_to_read 0 --query "
        SELECT countIf(message LIKE 'Failed to use set from cache, key: %') > 0
           AND countIf(message LIKE 'Building set, key: %') > 0
        FROM system.text_log
        WHERE event_date >= yesterday() AND logger_name = 'CreatingSetsTransform'
          AND query_id LIKE concat('${table_uuid}', '::${waiter_2}\\_%')")
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
