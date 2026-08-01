#!/usr/bin/env bash
# Tags: no-parallel, no-ordinary-database, no-fasttest, use-rocksdb
# Tag no-parallel: uses PAUSEABLE failpoints; concurrent test instances would share the
# same global failpoint channel and interfere with each other's ENABLE/DISABLE sequence.
# Tag no-ordinary-database: Alias resolves its target through the database catalog.
# Tag no-fasttest: In fasttest, ENABLE_LIBRARIES=0, so rocksdb engine is not enabled by default

# TRUNCATE TABLES FROM ... LIKE truncates every matched table concurrently on a thread pool that
# shares one query context, so several tasks can want the same target's exclusive lock at once.
# That is why StorageAlias::truncate asks for the lock under RWLockImpl::NO_QUERY: with the current
# query id, the second task would hit RWLockImpl::getLock's same-query fast path, which raises a
# LOGICAL_ERROR instead of waiting. The tasks finish in microseconds on their own, so two failpoints
# force the interleaving: $FP_TOP holds each task at the top of the per-table lambda until they are
# all in flight, then $FP_LOCK holds the first one WHILE IT OWNS the target's exclusive lock, so the
# next one provably requests a lock that is already held. Without the second failpoint the tasks
# merely overlap at the top of the lambda and the first can acquire and release before the second
# asks, which is the interleaving where the fast path is never reached.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

FP_TOP="truncate_database_tables_pause"
FP_LOCK="alias_truncate_pause_holding_target_lock"

function cleanup()
{
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT $FP_TOP" 2>/dev/null ||:
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT $FP_LOCK" 2>/dev/null ||:
    wait 2>/dev/null ||:
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS tt_alias_1; DROP TABLE IF EXISTS tt_alias_2; DROP TABLE IF EXISTS tt_rdb" 2>/dev/null ||:
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS tt_alias_1;
    DROP TABLE IF EXISTS tt_alias_2;
    DROP TABLE IF EXISTS tt_rdb;

    CREATE TABLE tt_rdb (k UInt64, v String) ENGINE = EmbeddedRocksDB PRIMARY KEY k;
    CREATE TABLE tt_alias_1 ENGINE = Alias($CLICKHOUSE_DATABASE, 'tt_rdb');
    CREATE TABLE tt_alias_2 ENGINE = Alias($CLICKHOUSE_DATABASE, 'tt_rdb');
"

# Run one TRUNCATE TABLES ... LIKE with every matched task provably in flight at the same time, and
# with one of them provably holding the target's exclusive lock while another requests it.
# $1 is the LIKE pattern, $2 the label.
function truncate_with_forced_overlap()
{
    local pattern="$1"
    local label="$2"
    local query_id="ttl_${label}_${CLICKHOUSE_DATABASE}"

    $CLICKHOUSE_CLIENT -q "INSERT INTO tt_rdb SELECT number, repeat('x', 200) FROM numbers(1000)"
    $CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT $FP_TOP"
    # Enabled before the statement starts, so the first task to pass $FP_TOP parks here holding the
    # lock. PAUSEABLE_ONCE: it fires for exactly one task, so the others are free to request the
    # lock and block on it rather than parking too.
    $CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT $FP_LOCK"

    $CLICKHOUSE_CLIENT --query_id="$query_id" -q "TRUNCATE TABLES FROM $CLICKHOUSE_DATABASE LIKE '$pattern'" 2>&1 &
    local truncate_pid=$!

    # Returns as soon as a task is parked, with no polling. Every matched task then piles up here,
    # because $FP_TOP is PAUSEABLE rather than PAUSEABLE_ONCE.
    $CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT $FP_TOP PAUSE"

    # Count the tasks that reached the failpoint. They are the only threads named TruncTbls, and
    # each is blocked inside notifyPauseAndWaitForResume, so this is a lower bound on the overlap.
    # system.stack_trace has an inherent race: each thread is signalled and awaited for only
    # storage_system_stack_trace_pipe_read_timeout_ms (100 ms by default), and on a miss an empty
    # trace is inserted for that thread, so a single shot can undercount. The tasks stay parked
    # until the failpoint is disabled, so retrying is free. Same reason as
    # 03565_system_stack_trace_works.sh, which retries 100 times for this table.
    local parked=0
    for _ in {1..50}; do
        parked=$($CLICKHOUSE_CLIENT -q "
            SELECT count() FROM system.stack_trace
            WHERE thread_name = 'TruncTbls' AND arrayExists(x -> position(x, 'FailPointInjection') > 0, arrayMap(y -> demangle(addressToSymbol(y)), trace))
            SETTINGS allow_introspection_functions = 1")
        [[ $parked -gt 1 ]] && break
        sleep 0.05
    done
    echo -e "$label tasks parked together\t$((parked > 1 ? 1 : 0))"

    # Release the tasks waiting at the top of the lambda. One of them then takes the target's
    # exclusive lock and parks at $FP_LOCK still holding it; this wait returns at that moment, so
    # from here until the disable below the lock is provably held while the others ask for it.
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT $FP_TOP"
    $CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT $FP_LOCK PAUSE"

    # Assert the interleaving itself rather than assuming it: one task parked at the failpoint while
    # still owning the target's exclusive lock, and another already inside RWLockImpl::getLock asking
    # for that same lock. This is the only state in which the query id passed to lockExclusively
    # matters, and it is a stable state -- both threads stay there until the failpoint is disabled --
    # so it can be polled. Without this pair asserted the cell could pass having only overlapped at
    # the top of the lambda, where the first task can acquire and release before the second asks.
    local held=0
    for _ in {1..50}; do
        held=$($CLICKHOUSE_CLIENT -q "
            SELECT countIf(arrayExists(x -> position(x, 'FailPointInjection') > 0, syms)) > 0
               AND countIf(arrayExists(x -> position(x, 'RWLockImpl::getLock') > 0, syms)) > 0
            FROM (
                SELECT arrayMap(y -> demangle(addressToSymbol(y)), trace) AS syms
                FROM system.stack_trace
                WHERE thread_name = 'TruncTbls'
                  AND arrayExists(x -> position(x, 'StorageAlias::truncate') > 0, arrayMap(y -> demangle(addressToSymbol(y)), trace))
            )
            SETTINGS allow_introspection_functions = 1")
        [[ $held -eq 1 ]] && break
        sleep 0.05
    done
    echo -e "$label lock held while another task requests it\t$held"

    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT $FP_LOCK"
    wait $truncate_pid && echo -e "$label truncate succeeded\t1"
    $CLICKHOUSE_CLIENT -q "SELECT '$label rows after truncate', count() FROM tt_rdb"
}

# Two aliases over one target: both tasks want the same target lock.
truncate_with_forced_overlap 'tt\_alias%' 'two aliases'

# An alias next to the target itself: one task locks the target directly, the other through the alias.
truncate_with_forced_overlap 'tt\_%' 'aliases with target'

# The target is still usable, so neither the storage nor its handle was lost.
# INSERT ... SELECT rather than INSERT ... VALUES: the runner redirects only stdout and stderr, so
# the client inherits the runner's stdin and a VALUES insert blocks on it until the test times out.
$CLICKHOUSE_CLIENT -q "
    INSERT INTO tt_rdb SELECT 1, 'a';
    SELECT 'direct', count() FROM tt_rdb;
    SELECT 'through alias', count() FROM tt_alias_1;
"
