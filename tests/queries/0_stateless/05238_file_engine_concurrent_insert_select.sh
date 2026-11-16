#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs the Parquet format

# INSERTs into a `File` table must not wait out `lock_acquire_timeout` behind a SELECT that reads the table more than once.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

URL="${CLICKHOUSE_URL}&lock_acquire_timeout=30"

# Races 12 INSERTs into table $1 against 4 copies of the SELECT $2; any error is printed.
race() {
    for _ in {1..12}; do
        ${CLICKHOUSE_CURL} -sS "$URL" -d "INSERT INTO $1 SETTINGS engine_file_truncate_on_insert = 1, async_insert = 0 VALUES (1, 'w')" &
    done
    for _ in {1..4}; do
        ${CLICKHOUSE_CURL} -sS "$URL" -d "$2 FORMAT Null" &
    done
    wait
    ${CLICKHOUSE_CLIENT} -q "SELECT count() > 0 FROM $1 WHERE y = 'w'"
}

${CLICKHOUSE_CLIENT} -q "
CREATE TABLE one (x UInt64, y String) ENGINE = File(Parquet);
CREATE TABLE many (x UInt64, y String) ENGINE = File(Parquet);
CREATE TABLE t (x UInt64) ENGINE = File(TSV);
INSERT INTO one VALUES (0, 'base');"
for i in {1..6}; do
    ${CLICKHOUSE_CLIENT} -q "INSERT INTO many SETTINGS engine_file_allow_create_multiple_files = 1 VALUES ($i, 'v')"
done
${CLICKHOUSE_CLIENT} -q "SELECT uniqExact(_file) FROM many"

race many "SELECT * FROM many SETTINGS query_plan_optimize_lazy_materialization_for_file = 0, max_threads = 8, max_threads_min_free_memory_per_thread = 0"
race one "SELECT * FROM one ORDER BY x LIMIT 2 SETTINGS query_plan_optimize_lazy_materialization = 1,
    query_plan_max_limit_for_lazy_materialization = 0, query_plan_optimize_lazy_materialization_for_file = 1"
race one "SELECT a.x, b.y FROM one AS a, one AS b SETTINGS query_plan_optimize_lazy_materialization_for_file = 0, max_threads = 1"

# A zero timeout still fails at once while another query holds the table, and succeeds once it is free.
${CLICKHOUSE_CLIENT} --query_id "$CLICKHOUSE_TEST_UNIQUE_NAME" -q "INSERT INTO t SELECT number FROM numbers(10)
    WHERE NOT sleepEachRow(1) SETTINGS max_block_size = 1" &
for _ in {1..300}; do
    [[ $(${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.processes WHERE query_id = '$CLICKHOUSE_TEST_UNIQUE_NAME' AND written_rows > 0") == 1 ]] && break
    sleep 0.1
done
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t SETTINGS lock_acquire_timeout = 0" 2>&1 | grep -m1 -oF TIMEOUT_EXCEEDED
wait
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t SETTINGS lock_acquire_timeout = 0"

# A query that reads and writes the same table fails at once instead of waiting for itself.
timeout 20 ${CLICKHOUSE_CLIENT} -q "INSERT INTO t SELECT * FROM t SETTINGS lock_acquire_timeout = 300" 2>&1 | grep -m1 -oF TIMEOUT_EXCEEDED
