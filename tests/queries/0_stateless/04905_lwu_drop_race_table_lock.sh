#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-replicated-database, no-shared-merge-tree
# Tag no-fasttest: relies on a failpoint (libfiu).
# Tag no-parallel: the test waits on a server-global pauseable failpoint, so a concurrent copy's
#   sink could satisfy this copy's wait and this copy's resume could release that sink.
#   FailPointInjection::{enable,disable}FailPoint take only a name, so it cannot be scoped.
# Tag no-replicated-database: the test needs its own Memory database, which a Replicated database
#   run cannot host, and DROP must reach the synchronous exclusive-lock path.
# Tag no-shared-merge-tree: the failpoint this test parks on is in the ReplicatedMergeTree sink,
#   which a SharedMergeTree table does not go through.

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

FP=rmt_pause_before_commit_local_part

# The failpoint is server-global, so leaving it enabled would park the sink of every later test.
# This has to survive the test failing or being killed part-way through.
function cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT ${FP}" < /dev/null 2>/dev/null || true
}
trap cleanup EXIT

# A Memory database has no UUID, so DROP takes the table's exclusive lock and removes the data
# inline instead of deferring it to the background cleanup an Atomic database uses.
${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS ${CLICKHOUSE_DATABASE_1}" < /dev/null
${CLICKHOUSE_CLIENT} --query "CREATE DATABASE ${CLICKHOUSE_DATABASE_1} ENGINE = Memory" < /dev/null

${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT ${FP}" < /dev/null 2>/dev/null || true

# Every DROP the test's logic depends on pins ignore_drop_queries_probability: the stress runner
# injects 0.2 and clickhouse-client --fake-drop (upgrade check) injects 1, and for a storage that
# keeps data on disk the injection returns success without dropping anything.
function setup_table()
{
    ${CLICKHOUSE_CLIENT} --query "
        DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE_1}.t SYNC;

        CREATE TABLE ${CLICKHOUSE_DATABASE_1}.t (id UInt64, c2 String)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/04905_$1/', '1')
        ORDER BY id
        SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

        INSERT INTO ${CLICKHOUSE_DATABASE_1}.t SELECT number, 'a' FROM numbers(2);
    " < /dev/null
}

# The DROP has to land between the patch-part rename and the commit. The sink parks there and stays
# parked until this function resumes it, so the window is held open instead of being a timed one the
# drop could miss.
function race_with_drop()
{
    local arm=$1
    shift
    local qid="${CLICKHOUSE_DATABASE_1}_${arm}_${RANDOM}${RANDOM}"
    local drop_qid="${qid}_drop"

    ${CLICKHOUSE_CLIENT} --query "SYSTEM ENABLE FAILPOINT ${FP}" < /dev/null

    # Everything below reads the armed state as evidence, so an arm that is not actually armed would
    # make the rest of the checks vacuous rather than failing.
    if [ "$(${CLICKHOUSE_CLIENT} --query "
        SELECT enabled FROM system.fail_points WHERE name = '${FP}'
        SETTINGS enable_parallel_replicas = 0" < /dev/null 2>/dev/null)" != 1 ]; then
        echo "$arm: the commit hook was not armed"
        return
    fi

    ${CLICKHOUSE_CLIENT} --query_id "$qid" "$@" < /dev/null > /dev/null 2>&1 &
    local updater=$!

    # Returns once the sink has parked, so the update still holds the lock it took for the pipeline.
    # An update that never reaches the hook would otherwise wait here forever.
    # shellcheck disable=SC2086 # CLICKHOUSE_CLIENT carries arguments and must word-split
    if ! timeout 60 ${CLICKHOUSE_CLIENT} --query "SYSTEM WAIT FAILPOINT ${FP} PAUSE" < /dev/null; then
        ${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT ${FP}" < /dev/null 2>/dev/null || true
        wait "$updater" 2>/dev/null || true
        echo "$arm: the sink never reached the commit hook"
        return
    fi

    # The wait above returns at once when nothing is parked, so it does not by itself establish that
    # the sink reached the hook. The failpoint is one-shot, so it reports itself disabled only after
    # something fired it.
    if [ "$(${CLICKHOUSE_CLIENT} --query "
        SELECT enabled FROM system.fail_points WHERE name = '${FP}'
        SETTINGS enable_parallel_replicas = 0" < /dev/null 2>/dev/null)" != 0 ]; then
        ${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT ${FP}" < /dev/null 2>/dev/null || true
        wait "$updater" 2>/dev/null || true
        echo "$arm: the sink never parked at the commit hook"
        return
    fi

    ${CLICKHOUSE_CLIENT} --query_id "$drop_qid" --query "
        DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE_1}.t SYNC" < /dev/null > /dev/null 2>&1 &
    local dropper=$!

    # The sink is still parked, so the drop reaches the table lock and blocks on it while the
    # window is held open.
    sleep 2

    ${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT ${FP}" < /dev/null

    local update=ok
    wait "$updater" 2>/dev/null || update=failed

    local drop=ok
    wait "$dropper" 2>/dev/null || drop=failed

    # The lock wait is charged to the statement that blocked, so a non-zero value here is this
    # drop's own wait on this table and no other writer can supply it. A drop that reached the lock
    # only after the commit released it reports zero and never covered the race.
    local waited
    waited=$(${CLICKHOUSE_CLIENT} --query "
        SYSTEM FLUSH LOGS query_log;
        SELECT ProfileEvents['RWLockWritersWaitMilliseconds'] FROM system.query_log
        WHERE query_id = '${drop_qid}' AND type = 'QueryFinish' AND event_date >= yesterday()
            AND current_database = currentDatabase()
        ORDER BY event_time_microseconds DESC LIMIT 1
        SETTINGS max_rows_to_read = 0, enable_parallel_replicas = 0" < /dev/null 2>/dev/null) || waited=""
    case "$waited" in
        '' | *[!0-9]*) waited=0 ;;
    esac

    if [ "$waited" -eq 0 ]; then
        echo "$arm: the drop did not wait for the table lock"
        return
    fi

    # A drop that reported success without removing the table took no lock and proved nothing.
    if [ "$drop" = ok ] && [ "$(${CLICKHOUSE_CLIENT} --query "
        EXISTS ${CLICKHOUSE_DATABASE_1}.t
        SETTINGS enable_parallel_replicas = 0" < /dev/null 2>/dev/null)" != 0 ]; then
        drop=ignored
    fi

    echo "$arm: update=$update drop=$drop"
}

setup_table update
race_with_drop update --enable_lightweight_update 1 \
    --query "UPDATE ${CLICKHOUSE_DATABASE_1}.t SET c2 = 'xx' WHERE id = 1"

setup_table alter
race_with_drop alter --enable_lightweight_update 1 --alter_update_mode 'lightweight_force' \
    --query "ALTER TABLE ${CLICKHOUSE_DATABASE_1}.t UPDATE c2 = 'xx' WHERE id = 1"

# An Alias table resolves a different storage, so the update has to hold the target's lock as well.
setup_table alias
${CLICKHOUSE_CLIENT} --allow_experimental_alias_table_engine 1 --query "
    DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE_1}.a SYNC;
    CREATE TABLE ${CLICKHOUSE_DATABASE_1}.a ENGINE = Alias('${CLICKHOUSE_DATABASE_1}', 't');
" < /dev/null
race_with_drop alias --allow_experimental_alias_table_engine 1 --enable_lightweight_update 1 \
    --query "UPDATE ${CLICKHOUSE_DATABASE_1}.a SET c2 = 'xx' WHERE id = 1"

${CLICKHOUSE_CLIENT} --query "DROP DATABASE ${CLICKHOUSE_DATABASE_1}" < /dev/null
