#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: polyglot requires Rust build

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The query text recorded in system.query_log must never contain the inline INSERT data, on the
# success path and on every failure path of the polyglot dialect. All the scenarios are triggered
# first and the log is flushed and checked once at the end: `SYSTEM FLUSH LOGS` is expensive under
# sanitizers and remote storage, so one shared flush-retry loop keeps the test fast.

POLY="--allow_experimental_polyglot_dialect 1 --dialect polyglot --polyglot_dialect postgresql"
poly_url="${CLICKHOUSE_URL}&allow_experimental_polyglot_dialect=1&dialect=polyglot&polyglot_dialect=postgresql&input_format_connection_handling=1&input_format_max_block_wait_ms=1000"

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

# 3. A polyglot INSERT the server rejects before parsing (the bundled dialects cannot transpile
# INSERT ... FORMAT): on the exception-before-start path no AST exists to tell the INSERT header
# apart from the data, so the server logs only the prefix that cannot contain a literal value.
exc_id="${CLICKHOUSE_DATABASE}_04843_exc_leak"
${CLICKHOUSE_CURL} -sS -X POST "${poly_url}&query_id=${exc_id}" -d 'INSERT INTO t FORMAT CSV
987654321' 2>&1 | grep -om1 "SYNTAX_ERROR"

# 4. The INSERT keyword does not have to come first: a CTE-wrapped `WITH ... INSERT` carries inline
# data too, so a failed one must be redacted the same way instead of being logged verbatim.
cte_id="${CLICKHOUSE_DATABASE}_04843_cte_leak"
${CLICKHOUSE_CURL} -sS -X POST "${poly_url}&query_id=${cte_id}" -d 'WITH s AS (SELECT 1) INSERT INTO t FORMAT CSV
987654322' 2>&1 | grep -om1 "SYNTAX_ERROR"

# 5. A raw FORMAT payload can start with anything, not only a VALUES-style literal: a CSV body made
# of letters has no parenthesis, quote, or digit for the redaction to cut at, so the text must be
# cut right after the `FORMAT <name>` header instead.
fmt_id="${CLICKHOUSE_DATABASE}_04843_fmt_leak"
${CLICKHOUSE_CURL} -sS -X POST "${poly_url}&query_id=${fmt_id}" -d 'INSERT INTO t FORMAT CSV
secretleakvalue' 2>&1 | grep -om1 "SYNTAX_ERROR"

# 6. `EXPLAIN INSERT ... VALUES` is an inline-data carrier too: the data belongs to the nested
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
        OR (query_id IN ('$exc_id', '$cte_id', '$fmt_id', '$explain_id') AND type = 'ExceptionBeforeStart')
        OR query_id = '$marker_id')")
    [ "$seen" = "6" ] && break
    sleep 0.3
done

echo "--- query_log omits the inline INSERT data (expect: 1) ---"
$CLICKHOUSE_CLIENT -q "SELECT query NOT LIKE '%123%' AND query ILIKE 'INSERT INTO%' FROM system.query_log WHERE query_id = '$log_id' AND type = 'QueryStart' AND current_database = currentDatabase()"

echo "--- oversized query in multiquery mode is rejected on the client, without a server round trip (expect: 0) ---"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.query_log WHERE query_id = '$oversized_id'"

echo "--- exception-before-start log omits the inline INSERT data (expect: 1) ---"
$CLICKHOUSE_CLIENT -q "SELECT query NOT LIKE '%987654321%' AND query ILIKE 'INSERT INTO%' FROM system.query_log WHERE query_id = '$exc_id' AND type = 'ExceptionBeforeStart' AND current_database = currentDatabase()"

echo "--- exception-before-start log omits the data of a CTE-wrapped INSERT (expect: 1) ---"
$CLICKHOUSE_CLIENT -q "SELECT query NOT LIKE '%987654322%' AND query ILIKE 'WITH%' FROM system.query_log WHERE query_id = '$cte_id' AND type = 'ExceptionBeforeStart' AND current_database = currentDatabase()"

echo "--- exception-before-start log omits a letters-only FORMAT payload (expect: 1) ---"
$CLICKHOUSE_CLIENT -q "SELECT query NOT LIKE '%secretleakvalue%' AND query ILIKE 'INSERT INTO%' FROM system.query_log WHERE query_id = '$fmt_id' AND type = 'ExceptionBeforeStart' AND current_database = currentDatabase()"

echo "--- the log of an EXPLAIN INSERT omits its inline data (expect: 1) ---"
$CLICKHOUSE_CLIENT -q "SELECT query NOT LIKE '%987654323%' AND query ILIKE 'EXPLAIN INSERT INTO%' FROM system.query_log WHERE query_id = '$explain_id' AND type = 'ExceptionBeforeStart' AND current_database = currentDatabase()"

echo "--- nothing was inserted by the failed statements (expect: 123 1) ---"
$CLICKHOUSE_CLIENT -q "SELECT sum(x), count() FROM t"

$CLICKHOUSE_CLIENT -q "DROP TABLE t"
