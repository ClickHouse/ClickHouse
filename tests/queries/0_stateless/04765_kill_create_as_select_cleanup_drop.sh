#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-ordinary-database, no-replicated-database
# no-fasttest: relies on a failpoint (libfiu).
# Tag no-parallel: the failpoint parks the single global background drop thread, which would stall
#   the drop queue of any concurrently running test.
# no-ordinary-database: a plain CREATE ... AS SELECT is only routed through the temporary-table
#   publish path (the code under test) on an Atomic database.
# no-replicated-database: the internal context takes a different branch when a Replicated-database
#   ZooKeeper transaction is present.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# A plain `CREATE TABLE ... AS SELECT` on an Atomic database populates a temporary table and
# publishes it with a RENAME; if the populate fails, a cleanup DROP removes the temporary table.
# That DROP runs on an internal context, and with
# database_atomic_wait_for_drop_and_detach_synchronously = 1 it waits in
# DatabaseCatalog::waitTableFinallyDropped until the background drop queue has finalized the
# table. The wait's only exit is the process list element of its context, so an internal context
# without one made the wait -- and therefore the whole CREATE -- unkillable.
#
# The wait is entered deterministically by parking the background drop thread at a failpoint
# placed after dropTableFinally() and before the UUID is erased from tables_marked_dropped_ids:
# the waiter's predicate still holds, so it cannot leave the loop on its own.

FP="database_catalog_drop_finally_before_id_erase"
QID="04765_${CLICKHOUSE_DATABASE}"
CH="${CLICKHOUSE_CLIENT}"

# Make sure the failpoint is off if a previous run left it enabled.
$CH -q "SYSTEM DISABLE FAILPOINT ${FP}" 2>/dev/null

# Warm the background drop task so the failpoint is reached promptly below.
$CH -q "CREATE TABLE ${CLICKHOUSE_DATABASE}.warm (x Int32) ENGINE = MergeTree ORDER BY tuple()"
$CH -q "DROP TABLE ${CLICKHOUSE_DATABASE}.warm SYNC"

$CH -q "SYSTEM ENABLE FAILPOINT ${FP}"

# The populate throws, so the cleanup DROP runs and waits for the background finalization that
# the failpoint is holding.
$CH --query_id="${QID}" --database_atomic_wait_for_drop_and_detach_synchronously=1 \
    -q "CREATE TABLE ${CLICKHOUSE_DATABASE}.tgt ENGINE = MergeTree ORDER BY tuple()
        AS SELECT throwIf(number = 2) AS x FROM numbers(3)" > /dev/null 2>&1 &
CREATE_PID=$!

# The drop thread is parked: the table is gone but its UUID is still in the queue's id set.
$CH -q "SYSTEM WAIT FAILPOINT ${FP} PAUSE"

# Wait until the CREATE is actually inside the wait, so the KILL cannot land before it.
for _ in {1..600}; do
    n=$($CH -q "SELECT count() FROM system.processes WHERE query_id = '${QID}'" 2>/dev/null)
    [ "$n" = "1" ] && break
    sleep 0.1
done

# ASYNC: a SYNC kill would itself block for as long as the query does, i.e. forever before the fix.
$CH -q "KILL QUERY WHERE query_id = '${QID}' ASYNC" > /dev/null

# Positive control that the kill landed. This is true on a pre-fix build too (the flag is set,
# the wait just ignores it), so it is a control rather than the discriminator below.
killed=0
for _ in {1..600}; do
    c=$($CH -q "SELECT is_cancelled FROM system.processes WHERE query_id = '${QID}'" 2>/dev/null)
    [ "$c" = "1" ] && { killed=1; break; }
    [ -z "$c" ] && { killed=1; break; }
    sleep 0.1
done
echo "kill_landed ${killed}"

# The discriminator: the query must return while the failpoint is still parked. Before the fix it
# stayed in the wait and only the release below could end it. Bounded so a pre-fix build fails
# fast with a diff instead of running into the runner's global timeout (which would also leave
# the failpoint enabled for every later test).
returned=0
for _ in {1..600}; do
    if ! kill -0 "$CREATE_PID" 2>/dev/null; then returned=1; break; fi
    sleep 0.1
done
echo "returned_while_blocked ${returned}"

$CH -q "SYSTEM DISABLE FAILPOINT ${FP}"
wait "$CREATE_PID" 2>/dev/null

# The cleanup DROP's error is logged and swallowed (the populate's own error is what reaches the
# client), so the cancellation is asserted from the log. The first line is the positive control
# that the wait was entered at all: without it a pass could just mean the cleanup never waited.
$CH -q "SYSTEM FLUSH LOGS text_log"
$CH -q "SELECT count() > 0 FROM system.text_log
        WHERE query_id = '${QID}' AND message LIKE '%to be finally dropped%'"
$CH -q "SELECT count() > 0 FROM system.text_log
        WHERE query_id = '${QID}' AND message LIKE '%Cannot DROP temporary table%QUERY_WAS_CANCELLED%'"

# The failed create leaves no table behind, and the temporary table is not stranded.
$CH -q "EXISTS ${CLICKHOUSE_DATABASE}.tgt"
$CH -q "SELECT count() FROM system.dropped_tables WHERE database = '${CLICKHOUSE_DATABASE}'"
