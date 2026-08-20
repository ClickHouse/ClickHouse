#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# The queries go over HTTP because the value is only observable on a thread that both initiates the
# query and performs the reads: over the native protocol the reads happen on a separate pipeline
# thread, so the row of the initiating thread reads nothing and would assert nothing. The four
# queries of an arm are sent as one curl invocation with --next, so they share a single keep-alive
# connection and therefore a single handler thread; the test asserts that precondition below rather
# than relying on the connection pool to reuse a thread by chance.
URL="${CLICKHOUSE_URL}&log_queries=1&log_query_threads=1&log_profile_events=1"

${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS t_progress_src;
    DROP TABLE IF EXISTS t_progress_dst;
    CREATE TABLE t_progress_src (id UInt64) ENGINE = MergeTree ORDER BY id;
    CREATE TABLE t_progress_dst (id UInt64) ENGINE = MergeTree ORDER BY id;
    INSERT INTO t_progress_src SELECT number FROM numbers(200000);
"

select_args=()
for i in 1 2 3 4; do
    [ "$i" -gt 1 ] && select_args+=(--next)
    select_args+=("${URL}&query_id=${CLICKHOUSE_DATABASE}_sel_$i"
                  --data-binary "SELECT sum(id) FROM t_progress_src SETTINGS max_threads = 1")
done
${CLICKHOUSE_CURL} -sSg "${select_args[@]}" > /dev/null

insert_args=()
for i in 1 2 3 4; do
    [ "$i" -gt 1 ] && insert_args+=(--next)
    insert_args+=("${URL}&query_id=${CLICKHOUSE_DATABASE}_ins_$i"
                  --data-binary "INSERT INTO t_progress_dst SELECT number FROM numbers(50000) SETTINGS max_insert_threads = 1, max_threads = 1")
done
${CLICKHOUSE_CURL} -sSg "${insert_args[@]}" > /dev/null

${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS query_log, query_thread_log"

# Precondition, checked rather than assumed: four successful queries, one reused handler thread.
# Without it the equalities below could hold trivially and the test would assert nothing.
echo 'SELECT arm: queries finished, distinct threads, distinct read_rows values'
${CLICKHOUSE_CLIENT} -q "
    SELECT
        countIf(ql.type = 'QueryFinish' AND ql.read_rows = 200000),
        uniqExact(qtl.thread_id),
        uniqExact(qtl.read_rows)
    FROM system.query_thread_log qtl
    JOIN system.query_log ql ON ql.query_id = qtl.query_id
    WHERE qtl.query_id LIKE '${CLICKHOUSE_DATABASE}_sel_%' AND qtl.thread_id = qtl.master_thread_id
"

echo 'INSERT arm: queries finished, distinct threads, distinct written_rows values'
${CLICKHOUSE_CLIENT} -q "
    SELECT
        countIf(ql.type = 'QueryFinish' AND ql.written_rows = 50000),
        uniqExact(qtl.thread_id),
        uniqExact(qtl.written_rows)
    FROM system.query_thread_log qtl
    JOIN system.query_log ql ON ql.query_id = qtl.query_id
    WHERE qtl.query_id LIKE '${CLICKHOUSE_DATABASE}_ins_%' AND qtl.thread_id = qtl.master_thread_id
"

# The same row's ProfileEvents come from the performance counters, which are reset per attach, so
# they are the reference the progress columns of that row must agree with.
echo 'SELECT arm: read_rows vs ProfileEvents, and vs query_log'
${CLICKHOUSE_CLIENT} -q "
    SELECT
        countIf(qtl.read_rows != CAST(qtl.ProfileEvents, 'Map(String, UInt64)')['SelectedRows']),
        countIf(qtl.read_bytes != CAST(qtl.ProfileEvents, 'Map(String, UInt64)')['SelectedBytes']),
        countIf(qtl.read_rows > ql.read_rows)
    FROM system.query_thread_log qtl
    JOIN system.query_log ql ON ql.query_id = qtl.query_id
    WHERE qtl.query_id LIKE '${CLICKHOUSE_DATABASE}_sel_%' AND qtl.thread_id = qtl.master_thread_id
      AND ql.type = 'QueryFinish'
"

echo 'INSERT arm: written_rows vs ProfileEvents, and vs query_log'
${CLICKHOUSE_CLIENT} -q "
    SELECT
        countIf(qtl.written_rows != CAST(qtl.ProfileEvents, 'Map(String, UInt64)')['InsertedRows']),
        countIf(qtl.written_rows > ql.written_rows)
    FROM system.query_thread_log qtl
    JOIN system.query_log ql ON ql.query_id = qtl.query_id
    WHERE qtl.query_id LIKE '${CLICKHOUSE_DATABASE}_ins_%' AND qtl.thread_id = qtl.master_thread_id
      AND ql.type = 'QueryFinish'
"

${CLICKHOUSE_CLIENT} -q "
    DROP TABLE t_progress_src;
    DROP TABLE t_progress_dst;
"
