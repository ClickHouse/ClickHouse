#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: polyglot requires Rust build

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The query text recorded in system.query_log must not contain the inline INSERT data when the
# statement was parsed successfully (the data boundary is known then). A polyglot query that fails
# before an AST exists is logged as is: the boundary is unknown without parsing, and this matches
# the native dialect, which also logs an unparseable INSERT verbatim. All the scenarios are
# triggered first and the log is flushed and checked once at the end: `SYSTEM FLUSH LOGS` is
# expensive under sanitizers and remote storage, so one shared flush-retry loop keeps the test fast.

POLY="--allow_experimental_polyglot_dialect 1 --dialect polyglot --polyglot_dialect postgresql"

$CLICKHOUSE_CLIENT -q "CREATE TABLE t (x Int32) ENGINE = MergeTree ORDER BY x"

# 1. A successful polyglot inline INSERT: the logged query is the INSERT header without the row
# values. 123 is a sentinel that only appears in the data section.
log_id="${CLICKHOUSE_DATABASE}_04843_log"
$CLICKHOUSE_CLIENT $POLY --query_id="$log_id" -q "INSERT INTO t VALUES (123)"

# 2. An oversized statement in --multiquery / script mode, where the client parses with
# allow_multi_statements enabled (which disables the generic per-query length limit): the client
# itself rejects it before transpiling or sending anything, so the query_id must never appear in
# query_log at all. If the rejection came from the server instead, query_log would record it as
# ExceptionBeforeStart.
big_values=$(seq 1 100 | sed 's/.*/(&)/' | paste -sd, -)
oversized_id="${CLICKHOUSE_DATABASE}_04843_oversized"
$CLICKHOUSE_CLIENT $POLY --multiquery --max_query_size 100 --query_id="$oversized_id" -q "INSERT INTO t VALUES $big_values" 2>&1 | grep -om1 "counts towards max_query_size"

# 3. `EXPLAIN INSERT ... VALUES` is an inline-data carrier too: the data belongs to the nested
# INSERT. All the places that locate the data boundary use `getInsertAST`, so the logged query text
# is cut at the data for this form as well (this one holds for the native dialect too).
explain_id="${CLICKHOUSE_DATABASE}_04843_explain_leak"
$CLICKHOUSE_CLIENT --query_id "$explain_id" -q "EXPLAIN INSERT INTO t VALUES (987654323)" 2>&1 | grep -om1 "INCORRECT_QUERY"

# A marker query issued after all the scenarios: once it is visible in query_log, the server has
# certainly logged everything it received before it, so the ABSENCE assertion for the oversized
# query cannot pass spuriously.
marker_id="${CLICKHOUSE_DATABASE}_04843_marker"
$CLICKHOUSE_CLIENT --query_id="$marker_id" -q "SELECT 1" > /dev/null

# A query_log entry can be written after the client has already received the response, so retry
# the flush until every expected entry (and the marker) shows up instead of assuming a single
# flush is enough.
for _ in {1..100}
do
    $CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
    seen=$($CLICKHOUSE_CLIENT -q "SELECT uniqExact(query_id) FROM system.query_log WHERE current_database = currentDatabase() AND (
        (query_id = '$log_id' AND type = 'QueryStart')
        OR (query_id = '$explain_id' AND type = 'ExceptionBeforeStart')
        OR query_id = '$marker_id')")
    [ "$seen" = "3" ] && break
    sleep 0.3
done

echo "--- query_log omits the inline INSERT data (expect: 1) ---"
$CLICKHOUSE_CLIENT -q "SELECT query NOT LIKE '%123%' AND query ILIKE 'INSERT INTO%' FROM system.query_log WHERE query_id = '$log_id' AND type = 'QueryStart' AND current_database = currentDatabase()"

echo "--- oversized query in multiquery mode is rejected on the client, without a server round trip (expect: 0) ---"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.query_log WHERE query_id = '$oversized_id'"

echo "--- the log of an EXPLAIN INSERT omits its inline data (expect: 1) ---"
$CLICKHOUSE_CLIENT -q "SELECT query NOT LIKE '%987654323%' AND query ILIKE 'EXPLAIN INSERT INTO%' FROM system.query_log WHERE query_id = '$explain_id' AND type = 'ExceptionBeforeStart' AND current_database = currentDatabase()"

echo "--- nothing was inserted by the failed statements (expect: 123 1) ---"
$CLICKHOUSE_CLIENT -q "SELECT sum(x), count() FROM t"

$CLICKHOUSE_CLIENT -q "DROP TABLE t"
