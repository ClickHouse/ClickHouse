#!/usr/bin/env bash
# Tags: no-parallel, no-ordinary-database, no-fasttest, use-rocksdb
# Tag no-parallel: uses a PAUSEABLE failpoint; concurrent test instances would share the
# same global failpoint channel and interfere with each other's ENABLE/DISABLE sequence.
# Tag no-ordinary-database: Alias resolves its target through the database catalog.
# Tag no-fasttest: In fasttest, ENABLE_LIBRARIES=0, so rocksdb engine is not enabled by default

# TRUNCATE TABLES FROM ... LIKE truncates every matched table concurrently on a thread pool that
# shares one query context, so several tasks can want the same target's exclusive lock at once.
# That is why StorageAlias::truncate asks for the lock under RWLockImpl::NO_QUERY: with the current
# query id, the second task would hit RWLockImpl::getLock's same-query fast path, which raises a
# LOGICAL_ERROR instead of waiting. The tasks finish in microseconds on their own, so a PAUSEABLE
# failpoint holds every one of them at the top of the per-table lambda until they are all in flight.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

FP="truncate_database_tables_pause"

function cleanup()
{
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT $FP" 2>/dev/null ||:
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

# Run one TRUNCATE TABLES ... LIKE with every matched task provably in flight at the same time.
# $1 is the LIKE pattern, $2 the label. The failpoint blocks each task before it truncates, so the
# statement cannot finish until the failpoint is disabled and all tasks are released together.
function truncate_with_forced_overlap()
{
    local pattern="$1"
    local label="$2"
    local query_id="ttl_${label}_${CLICKHOUSE_DATABASE}"

    $CLICKHOUSE_CLIENT -q "INSERT INTO tt_rdb SELECT number, repeat('x', 200) FROM numbers(1000)"
    $CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT $FP"

    $CLICKHOUSE_CLIENT --query_id="$query_id" -q "TRUNCATE TABLES FROM $CLICKHOUSE_DATABASE LIKE '$pattern'" 2>&1 &
    local truncate_pid=$!

    # Returns as soon as a task is parked, with no polling. Every matched task then piles up here,
    # because the failpoint is PAUSEABLE rather than PAUSEABLE_ONCE.
    $CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT $FP PAUSE"

    # Count the tasks that reached the failpoint. They are the only threads named TruncTbls, and
    # each is blocked inside notifyPauseAndWaitForResume, so this is a lower bound on the overlap.
    local parked
    parked=$($CLICKHOUSE_CLIENT -q "
        SELECT count() FROM system.stack_trace
        WHERE thread_name = 'TruncTbls' AND arrayExists(x -> position(x, 'FailPointInjection') > 0, arrayMap(y -> demangle(addressToSymbol(y)), trace))
        SETTINGS allow_introspection_functions = 1")
    echo -e "$label tasks parked together\t$((parked > 1 ? 1 : 0))"

    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT $FP"
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
