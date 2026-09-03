#!/usr/bin/env bash
# Tags: no-random-settings
# `async_insert_parse_threads` is randomized by clickhouse-test, and this test sets it explicitly.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE t_parse_threads
    (
        id UInt64,
        arr Array(UInt64),
        s String DEFAULT concat('d', toString(id))
    )
    ENGINE = MergeTree ORDER BY id;

    SYSTEM STOP MERGES t_parse_threads;
"

# The settings below are part of the key of the asynchronous insert queue, so all the inserts of one
# loop iteration are collected into a single batch. Auto-flushing is effectively disabled, and the
# batch is flushed explicitly, which makes the number of resulting parts deterministic.
batch_settings="async_insert=1&wait_for_async_insert=0&async_insert_use_adaptive_busy_timeout=0&async_insert_busy_timeout_ms=600000&async_insert_max_data_size=1000000000&async_insert_max_query_number=1000000"

# 0 and 1 parse in the flushing thread, 4 splits 9 entries into 4 ranges, and 16 is clamped to the
# 9 entries of the batch.
for threads in 0 1 4 16
do
    url="${CLICKHOUSE_URL}&${batch_settings}&async_insert_parse_threads=${threads}"

    for i in $(seq 1 9)
    do
        # `s` is omitted, so every range has to build its own AddingDefaultsTransform.
        ${CLICKHOUSE_CURL} -sS "$url" -d "INSERT INTO t_parse_threads FORMAT JSONEachRow {\"id\": ${i}, \"arr\": [${i}, ${i}]}"
    done

    ${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH ASYNC INSERT QUEUE t_parse_threads"
done

echo "-- all the rows of every batch are inserted, whoever parsed them"
${CLICKHOUSE_CLIENT} -q "SELECT count(), sum(id), sum(arr[1] + arr[2]), uniqExact(s) FROM t_parse_threads"
${CLICKHOUSE_CLIENT} -q "SELECT DISTINCT id, arr, s FROM t_parse_threads ORDER BY id"

echo "-- one flush is one part, no matter how many threads parsed its data"
${CLICKHOUSE_CLIENT} -q "
    SELECT count(), groupUniqArray(rows)
    FROM system.parts
    WHERE database = currentDatabase() AND table = 't_parse_threads' AND active
"

echo "-- a row that does not parse fails only its own insert"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_parse_threads_err (id UInt64) ENGINE = MergeTree ORDER BY id"

url="${CLICKHOUSE_URL}&${batch_settings}&async_insert_parse_threads=4"
for i in $(seq 1 4)
do
    ${CLICKHOUSE_CURL} -sS "$url" -d "INSERT INTO t_parse_threads_err FORMAT JSONEachRow {\"id\": ${i}}"
done
${CLICKHOUSE_CURL} -sS "$url" -d 'INSERT INTO t_parse_threads_err FORMAT JSONEachRow {"id": "not a number"}'
${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH ASYNC INSERT QUEUE t_parse_threads_err"

${CLICKHOUSE_CLIENT} -q "SELECT count(), sum(id) FROM t_parse_threads_err"

# The log elements are queued by the flush, so retry until all five entries are visible.
for _ in {1..100}
do
    ${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS asynchronous_insert_log"
    logged=$(${CLICKHOUSE_CLIENT} -q "
        SELECT count()
        FROM system.asynchronous_insert_log
        WHERE database = currentDatabase() AND table = 't_parse_threads_err'
    ")
    if [[ "$logged" -ge 5 ]]
    then
        break
    fi
    sleep 0.1
done

${CLICKHOUSE_CLIENT} -q "
    SELECT status, count(), sum(rows)
    FROM system.asynchronous_insert_log
    WHERE database = currentDatabase() AND table = 't_parse_threads_err'
    GROUP BY status
    ORDER BY status
"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_parse_threads; DROP TABLE t_parse_threads_err"
