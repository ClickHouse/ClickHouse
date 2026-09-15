#!/usr/bin/env bash
# Tags: no-replicated-database, zookeeper, memory-engine, log-engine
#
# `TRUNCATE` of a MergeTree table in a `lazy_load_tables = 1` database must skip the exclusive
# lock, exactly as it does for an eagerly loaded MergeTree. `InterpreterDropQuery` classified
# the exemption on the raw catalog object, which for a lazy database is a `StorageTableProxy`
# and not a `MergeTreeData`, so the cast failed and the proxy's exclusive `drop_lock` was taken.
# Readers lock that same object, so the truncate waited for them and failed with
# `DEADLOCK_AVOIDED` once `lock_acquire_timeout` expired.
#
# Each arm starts a long reader, waits until it has actually read a row, then truncates with a
# short `lock_acquire_timeout`. `blocked=0` means the truncate skipped the exclusive lock.
# Arms C, D and E are controls. C and D pin the eager cases: a non-MergeTree table must still be
# blocked (the fix is not a blanket lock removal) and a MergeTree must stay exempt. E is the lazy
# non-MergeTree case and must be blocked as well, so that the classification cannot degrade into
# "exempt every proxy", which is the one mutant arms A to D all accept.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

LAZY="${CLICKHOUSE_DATABASE}_lazy"
PLAIN="${CLICKHOUSE_DATABASE}_plain"

cleanup() {
    ${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS \`${LAZY}\` SYNC" 2>/dev/null
    ${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS \`${PLAIN}\` SYNC" 2>/dev/null
}
trap cleanup EXIT
cleanup

${CLICKHOUSE_CLIENT} -nq "
    CREATE DATABASE \`${LAZY}\` ENGINE = Atomic SETTINGS lazy_load_tables = 1;
    CREATE TABLE \`${LAZY}\`.mt (id UInt64) ENGINE = MergeTree ORDER BY id;
    INSERT INTO \`${LAZY}\`.mt SELECT number FROM numbers(300);
    CREATE TABLE \`${LAZY}\`.rmt (id UInt64)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/rmt', 'r1')
        ORDER BY id;
    INSERT INTO \`${LAZY}\`.rmt SELECT number FROM numbers(300);
    CREATE TABLE \`${LAZY}\`.log (id UInt64) ENGINE = Log;
    INSERT INTO \`${LAZY}\`.log SELECT number FROM numbers(300);
    CREATE DATABASE \`${PLAIN}\` ENGINE = Atomic;
    CREATE TABLE \`${PLAIN}\`.mem (id UInt64) ENGINE = Memory;
    INSERT INTO \`${PLAIN}\`.mem SELECT number FROM numbers(300);
    CREATE TABLE \`${PLAIN}\`.mt (id UInt64) ENGINE = MergeTree ORDER BY id;
    INSERT INTO \`${PLAIN}\`.mt SELECT number FROM numbers(300);
"

# The inserts materialize the proxies, so re-attach to get them back. Assert the fixture is really
# a proxy before probing: if it reads `MergeTree` the arms would pass without exercising the fix.
# Query `engine` only: `parts` and `data_paths` materialize the proxy.
# `log` is asserted here for the same reason, and the assertion is load-bearing rather than
# decorative: an eagerly loaded Log is blocked too, so without it arm E keeps passing while no longer
# covering a proxy at all. If lazy loading ever stops admitting non-MergeTree engines this line is
# meant to fail, because that retires arm E rather than weakening it.
${CLICKHOUSE_CLIENT} -q "DETACH DATABASE \`${LAZY}\` SYNC"
${CLICKHOUSE_CLIENT} -q "ATTACH DATABASE \`${LAZY}\`"
${CLICKHOUSE_CLIENT} -q "SELECT name, engine FROM system.tables WHERE database = '${LAZY}' ORDER BY name"

# `arm <label> <table>`: prints `<label> blocked=<0|1> rows_after=<n>`.
arm() {
    local label="$1" tbl="$2"
    local qid="${CLICKHOUSE_TEST_UNIQUE_NAME}_${label}"

    ${CLICKHOUSE_CLIENT} --query_id "${qid}" -q "
        SELECT sum(sleepEachRow(0.2)) FROM ${tbl}
        SETTINGS max_block_size = 1, max_threads = 1,
                 function_sleep_max_microseconds_per_block = 100000000
    " >/dev/null 2>&1 &
    local reader_pid=$!

    # Handshake: the reader must hold its share lock and have started reading.
    local started=0
    for _ in $(seq 1 200); do
        if [ "$(${CLICKHOUSE_CLIENT} -q \
                "SELECT count() FROM system.processes WHERE query_id = '${qid}' AND read_rows > 0" \
                2>/dev/null)" = "1" ]; then
            started=1
            break
        fi
        sleep 0.1
    done
    if [ "${started}" != "1" ]; then
        echo "${label} ERROR reader did not start"
        return
    fi

    local err
    err=$(${CLICKHOUSE_CLIENT} -q "TRUNCATE TABLE ${tbl} SETTINGS lock_acquire_timeout = 3" 2>&1)
    local blocked=0
    # Assert the full message shape, not just the code, so the arm cannot pass for another reason.
    if echo "${err}" | grep -q "WRITE locking attempt on .* has timed out.*Possible deadlock avoided"; then
        blocked=1
    elif [ -n "${err}" ]; then
        echo "${label} UNEXPECTED $(echo "${err}" | head -1)"
    fi

    ${CLICKHOUSE_CLIENT} -q "KILL QUERY WHERE query_id = '${qid}' SYNC" >/dev/null 2>&1
    wait "${reader_pid}" 2>/dev/null

    echo "${label} blocked=${blocked} rows_after=$(${CLICKHOUSE_CLIENT} -q "SELECT count() FROM ${tbl}")"
}

arm A_lazy_mergetree            "\`${LAZY}\`.mt"
arm B_lazy_replicated_mergetree "\`${LAZY}\`.rmt"
arm C_plain_memory              "\`${PLAIN}\`.mem"
arm D_plain_mergetree           "\`${PLAIN}\`.mt"
arm E_lazy_log                  "\`${LAZY}\`.log"
