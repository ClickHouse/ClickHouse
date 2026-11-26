#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-ordinary-database, no-replicated-database
# no-fasttest: relies on a failpoint (libfiu).
# no-parallel, no-ordinary-database, no-replicated-database: uses an explicit fixed UUID and the
#   global background drop queue, neither of which may be shared with other tests.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Two tables that reuse one explicit UUID can both sit in the background drop queue at the same
# time (CREATE OR REPLACE TABLE with a fixed UUID enqueues an intermediate table carrying that
# UUID). tables_marked_dropped_ids must track them per occurrence; with a plain set the first
# drop-finalize erased the single membership record and the second tripped chassert('removed') in
# dropTablesParallel, aborting the server in debug builds.
#
# The two entries only ever coexist in tables_marked_dropped_ids, never in the observable
# tables_marked_dropped list: the global UUID mapping serializes same-UUID creates, and it is
# released only inside dropTableFinally(), after the entry has already been spliced out of the
# list. So the duplicate state is reproduced deterministically with a failpoint that pauses the
# drop thread in dropTablesParallel right after dropTableFinally() (which drops the UUID mapping)
# and before the guarded erase from tables_marked_dropped_ids.

FP="database_catalog_drop_finally_before_id_erase"
UUID="00004327-0000-4000-8000-000043270001"
CH="${CLICKHOUSE_CLIENT}"

# Make sure the failpoint is off if a previous run left it enabled.
$CH -q "SYSTEM DISABLE FAILPOINT ${FP}" 2>/dev/null

# First holder of the shared UUID. Log stores data on disk, so its drop is finalized by the
# background queue (in-memory engines are finalized inline and never enter the queue).
$CH -q "CREATE TABLE ${CLICKHOUSE_DATABASE}.a UUID '${UUID}' (x Int32) ENGINE = Log"

$CH -q "SYSTEM ENABLE FAILPOINT ${FP}"

# DROP SYNC enqueues 'a' for immediate background finalization. The drop thread removes the UUID
# mapping in dropTableFinally(), then pauses at the failpoint before erasing the membership record.
# It runs in the background because SYNC blocks until 'a' has fully drained from the queue.
$CH -q "DROP TABLE ${CLICKHOUSE_DATABASE}.a SYNC" &
DROP_A_PID=$!

# Wait until the drop thread is parked at the failpoint: the UUID mapping is now gone, but the
# membership record for ${UUID} is still present.
$CH -q "SYSTEM WAIT FAILPOINT ${FP} PAUSE"

# The UUID is free again, so a second table can take it. This CREATE succeeds only because 'a' is
# mid-drop with its mapping already removed; with 'a' fully in the queue it would fail with
# "Mapping for table with UUID=... already exists". Its success is the observable signal that two
# entries now share ${UUID}.
$CH -q "CREATE TABLE ${CLICKHOUSE_DATABASE}.b UUID '${UUID}' (x Int32) ENGINE = Log"
$CH -q "EXISTS ${CLICKHOUSE_DATABASE}.b"

# Enqueue 'b' for finalization too (SYNC -> no drop delay -> processed right after 'a' resumes).
# The single drop task is parked on 'a', so 'b' cannot be finalized before 'a' finishes.
$CH -q "DROP TABLE ${CLICKHOUSE_DATABASE}.b SYNC" &
DROP_B_PID=$!

# Make sure 'b' has actually been enqueued (it shows up in tables_marked_dropped) before resuming,
# so the two same-UUID entries are guaranteed to coexist in tables_marked_dropped_ids.
for _ in {1..100}; do
    n=$($CH -q "SELECT count() FROM system.dropped_tables WHERE uuid = '${UUID}'" 2>/dev/null)
    [ "$n" = "1" ] && break
    sleep 0.1
done

# Resume. 'a' erases one membership record; then 'b' is finalized and must still find its own.
# Without the per-occurrence fix the second erase found nothing and tripped chassert('removed').
$CH -q "SYSTEM DISABLE FAILPOINT ${FP}"

wait "$DROP_A_PID"
wait "$DROP_B_PID"

# If the server aborted on the chassert, this query fails and the test reports the crash.
$CH -q "SELECT 1"
