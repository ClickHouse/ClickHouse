#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs the Parquet format

# Concurrent INSERTs and SELECTs on one `File` table must all finish, instead of the INSERTs
# waiting out `lock_acquire_timeout` and being rejected with TIMEOUT_EXCEEDED. One SELECT can open
# several readers on the same table, and each arm below is a different way to get there: reading
# more than one file in parallel, an `ORDER BY ... LIMIT` that reads the remaining columns in a
# second pass, and a self-join that reads the table twice. The last section pins the boundary that
# must not move: `lock_acquire_timeout = 0` still means "do not wait at all".

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

WRITERS=12
READERS=4
# Long enough that ordinary contention between the writers never reaches it on a loaded runner (a
# healthy wave of all 16 clients finishes in well under a second, and the slowest sanitizer runners
# are several times slower than that), short enough that three arms of an unfixed server each burn
# the full timeout well inside the runner's own time budget.
LOCK_TIMEOUT=30

q() { ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "$1"; }

# Races $WRITERS INSERTs against $READERS copies of one SELECT and reports whether every client
# succeeded. $1 = arm name, $2 = table to insert into, $3 = the SELECT to race against it.
arm() {
    local name="$1" table="$2" select="$3"
    local dir="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_${name}"
    rm -rf "$dir"; mkdir -p "$dir"

    local pids=() i rc=0
    for i in $(seq 1 $WRITERS); do
        ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "
            INSERT INTO ${table}
            SETTINGS engine_file_truncate_on_insert = 1, async_insert = 0, lock_acquire_timeout = ${LOCK_TIMEOUT}
            VALUES (1, 'w')" > "$dir/w$i" 2>&1 &
        pids+=($!)
    done
    for i in $(seq 1 $READERS); do
        ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "$select" > "$dir/r$i" 2>&1 &
        pids+=($!)
    done
    for i in "${pids[@]}"; do wait "$i" || rc=1; done

    # Silence from the clients is not enough to report success: a client that died below the HTTP
    # layer prints text that matches neither pattern below, and one that never reached the server
    # leaves an empty file. Every writer above truncates the first file and leaves its own single
    # row behind, so after the wave the table holds at least one of them.
    local written
    written=$(${CLICKHOUSE_CLIENT} -q "SELECT count() FROM ${table} WHERE y = 'w'" 2>/dev/null)
    if grep -qE 'Code: [0-9]+|Exception' "$dir"/* 2>/dev/null; then
        echo "$name FAILED"
        grep -hoE 'Code: [0-9]+[^,]*' "$dir"/* 2>/dev/null | sort | uniq -c
    elif [[ "$rc" != 0 ]]; then
        echo "$name FAILED: a client exited non-zero"
    elif [[ "$written" -ge 1 ]]; then
        echo ok
    else
        echo "$name FAILED: no writer reached the server (rows written: '$written')"
    fi
    rm -rf "$dir"
}

${CLICKHOUSE_CLIENT} --multiline -q "
DROP TABLE IF EXISTS one;
DROP TABLE IF EXISTS many;
DROP TABLE IF EXISTS zerotimeout;
DROP TABLE IF EXISTS selfinsert;
CREATE TABLE one (x UInt64, y String) ENGINE = File(Parquet);
CREATE TABLE many (x UInt64, y String) ENGINE = File(Parquet);
INSERT INTO one VALUES (0, 'base');
"
# Six files in one table, so that a reader with several streams opens a reader per file.
for i in 1 2 3 4 5 6; do
    q "INSERT INTO many SETTINGS engine_file_allow_create_multiple_files = 1 VALUES ($i, 'v$i')"
done
# The arm below needs one read source per file, and it is `engine_file_allow_create_multiple_files`
# above that splits them. Six rows are not six files: a setup that silently left one file would leave
# one source, and an arm that passes without the query ever asking for a second read lock.
${CLICKHOUSE_CLIENT} -q "SELECT uniqExact(_file) FROM many"

# Several streams over several files. The number of streams is what makes this arm read the table more
# than once, so the setting that lowers it when the server is short of free memory is turned off here.
arm many many "SELECT x, y FROM many
    SETTINGS query_plan_optimize_lazy_materialization_for_file = 0, max_threads = 8,
             max_threads_min_free_memory_per_thread = 0,
             lock_acquire_timeout = ${LOCK_TIMEOUT} FORMAT Null"

# A single file read whose remaining columns are fetched in a second pass. The pass and its row
# limit are pinned because the runner otherwise turns them off in some runs, which would leave this
# arm reading the table once and exercising nothing. `enable_analyzer` is pinned because lazy
# materialization requires the analyzer; v26.9 and newer make it mandatory, so the pin only states
# that requirement.
arm lazy one "SELECT x, y FROM one ORDER BY x LIMIT 2
    SETTINGS enable_analyzer = 1,
             query_plan_optimize_lazy_materialization = 1, query_plan_max_limit_for_lazy_materialization = 0,
             query_plan_optimize_lazy_materialization_for_file = 1,
             lock_acquire_timeout = ${LOCK_TIMEOUT} FORMAT Null"

# One table read twice by one query, on a single thread.
arm selfjoin one "SELECT a.x, b.y FROM one AS a, one AS b
    SETTINGS query_plan_optimize_lazy_materialization_for_file = 0, max_threads = 1,
             lock_acquire_timeout = ${LOCK_TIMEOUT} FORMAT Null"

# A zero `lock_acquire_timeout` must not wait for a lock somebody else holds, and must not turn
# into "wait forever" either. `max_block_size = 1` keeps each block's sleep under
# `function_sleep_max_microseconds_per_block`, and `NOT sleepEachRow(...)` is the form that is not
# optimized away.
${CLICKHOUSE_CLIENT} -q "CREATE TABLE zerotimeout (x UInt64, y String) ENGINE = File(TSV)"
hold_id="${CLICKHOUSE_TEST_UNIQUE_NAME}_hold"
${CLICKHOUSE_CLIENT} --query_id "$hold_id" -q "INSERT INTO zerotimeout SELECT number, 'w' FROM numbers(10)
                         WHERE NOT sleepEachRow(1) SETTINGS max_block_size = 1" &
writer=$!
# The sink takes the table before the first row reaches it, so a written row proves the INSERT is holding
# it, with most of its ten seconds still to run while the two probes below go through.
held=0
for _ in $(seq 1 300); do
    [[ "$(${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.processes
          WHERE query_id = '$hold_id' AND written_rows > 0")" == "1" ]] && { held=1; break; }
    sleep 0.1
done
[[ "$held" == 1 ]] && echo held || echo "FAILED: the INSERT never took the table"
# 1: a zero timeout does not wait for the INSERT to release the table
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM zerotimeout SETTINGS lock_acquire_timeout = 0" 2>&1 \
    | grep -qF 'TIMEOUT_EXCEEDED' && echo 1 || echo 0
# ok: a reader willing to wait still gets in
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM zerotimeout SETTINGS lock_acquire_timeout = 300" > /dev/null && echo ok
wait $writer
# 10: with nobody holding the table a zero timeout succeeds
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM zerotimeout SETTINGS lock_acquire_timeout = 0"

# One query that needs this table in both modes must fail at once instead of waiting out the timeout. The
# outer deadline is far below `lock_acquire_timeout`, so waiting, a different error and a hang all fail.
${CLICKHOUSE_CLIENT} -q "CREATE TABLE selfinsert (x UInt64, y String) ENGINE = File(TSV)"
${CLICKHOUSE_CLIENT} -q "INSERT INTO selfinsert VALUES (1, 'a')"
timeout 20 ${CLICKHOUSE_CLIENT} -q "INSERT INTO selfinsert SELECT x, y FROM selfinsert
                                    SETTINGS lock_acquire_timeout = 300" 2>&1 \
    | grep -qF 'TIMEOUT_EXCEEDED' && echo ok || echo "selfinsert FAILED"

${CLICKHOUSE_CLIENT} --multiline -q "DROP TABLE one; DROP TABLE many; DROP TABLE zerotimeout; DROP TABLE selfinsert;"
