#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: polyglot requires Rust build

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

POLY="--allow_experimental_polyglot_dialect 1 --dialect polyglot --polyglot_dialect postgresql"

$CLICKHOUSE_CLIENT -q "CREATE TABLE t (x Int32) ENGINE = MergeTree ORDER BY x"

# INSERT ... VALUES with inline data through the polyglot (PostgreSQL) dialect. The whole
# statement, including the inline VALUES data, is sent to the server verbatim; the server
# transpiles it and reads the data from the transpiled query. Both multi-row and single-row.
$CLICKHOUSE_CLIENT $POLY -q "INSERT INTO t VALUES (1), (2), (3)"
$CLICKHOUSE_CLIENT $POLY -q "INSERT INTO t VALUES (4)"

echo "--- after VALUES inserts (expect: 10 4) ---"
$CLICKHOUSE_CLIENT -q "SELECT sum(x), count() FROM t"

# INSERT ... SELECT (no inline data) still works through the dialect.
$CLICKHOUSE_CLIENT $POLY -q "INSERT INTO t SELECT 90"
echo "--- after INSERT SELECT (expect: 100 5) ---"
$CLICKHOUSE_CLIENT -q "SELECT sum(x), count() FROM t"

# PostgreSQL boolean literals are transpiled inside the VALUES data.
$CLICKHOUSE_CLIENT -q "CREATE TABLE b (flag UInt8) ENGINE = MergeTree ORDER BY flag"
$CLICKHOUSE_CLIENT $POLY -q "INSERT INTO b VALUES (true), (false)"
echo "--- boolean transpile (expect: 1 2) ---"
$CLICKHOUSE_CLIENT -q "SELECT sum(flag), count() FROM b"

# The inline INSERT data must never reach system.query_log/processlist: the "INSERT logs omit
# inserted data" contract applies to the polyglot dialect too. The logged query is the INSERT
# header (target and, if any, column list) without the row values. 123 is a sentinel that only
# appears in the data section, so it must be absent from the logged query.
query_id="${CLICKHOUSE_DATABASE}_04512_log"
$CLICKHOUSE_CLIENT $POLY --query_id="$query_id" -q "INSERT INTO b VALUES (123)"
$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
echo "--- query_log omits the inline INSERT data (expect: 1) ---"
$CLICKHOUSE_CLIENT -q "SELECT query NOT LIKE '%123%' AND query ILIKE 'INSERT INTO%' FROM system.query_log WHERE query_id = '$query_id' AND type = 'QueryStart' AND current_database = currentDatabase()"

# Multi-statement input is still rejected in polyglot dialect.
$CLICKHOUSE_CLIENT $POLY -q "SELECT 1; SELECT 2" 2>&1 | grep -om1 "SYNTAX_ERROR"

# A statement following inline INSERT ... VALUES data is also rejected cleanly (the whole
# multi-statement buffer is transpiled at once, and the transpiler rejects it) rather than
# being silently mis-executed or reaching the server as unread VALUES tail.
$CLICKHOUSE_CLIENT $POLY -q "INSERT INTO t VALUES (1); SELECT 2" 2>&1 | grep -om1 "SYNTAX_ERROR"
echo "--- no partial insert after rejected multi-statement (expect: 100 5) ---"
$CLICKHOUSE_CLIENT -q "SELECT sum(x), count() FROM t"

# External insert data (piped stdin) together with a polyglot INSERT is rejected instead of
# being silently dropped: the query is sent to the server verbatim and the client never
# forwards stdin data, so accepting it would lose the piped rows.
printf '(6)\n' | $CLICKHOUSE_CLIENT $POLY -q "INSERT INTO t VALUES (5)" 2>&1 | grep -om1 "NOT_IMPLEMENTED"
echo "--- no insert when external stdin data is rejected (expect: 100 5) ---"
$CLICKHOUSE_CLIENT -q "SELECT sum(x), count() FROM t"

# EXPLAIN wrapping a plain SELECT works through the polyglot dialect: the parser recognizes an
# EXPLAIN-wrapped statement that is not an INSERT (so it has no inline data to clear) and leaves it
# to the normal flow.
$CLICKHOUSE_CLIENT $POLY -q "EXPLAIN SELECT 1" > /dev/null && echo "--- EXPLAIN SELECT works (expect: ok) ---" && echo ok

# EXPLAIN INSERT ... VALUES with inline data is currently rejected by the transpiler (no bundled
# dialect transpiles it), so it fails cleanly with a syntax error and inserts nothing - never a
# use-after-free of the transient transpiled buffer. The parser still defensively clears the
# inline-data pointers of an EXPLAIN-wrapped INSERT (the same way the client unwraps it in
# ClientBase::analyzeMultiQueryText), in case a future transpiler starts supporting this form.
$CLICKHOUSE_CLIENT $POLY --multiquery -q "EXPLAIN INSERT INTO t VALUES (1)" 2>&1 | grep -om1 "SYNTAX_ERROR"
echo "--- no insert from rejected EXPLAIN INSERT (expect: 100 5) ---"
$CLICKHOUSE_CLIENT -q "SELECT sum(x), count() FROM t"

# INSERT ... FORMAT with inline data is not transpilable by the bundled foreign dialects. FORMAT is a
# ClickHouse-only extension: a foreign-dialect parser has no notion of it, so it rejects the query at
# the inline data that follows (the PostgreSQL dialect here fails on the first data row after
# FORMAT CSV). A foreign-dialect INSERT ... FORMAT therefore fails cleanly with a syntax error and
# inserts nothing, exactly like EXPLAIN INSERT ... VALUES above. Only INSERT ... VALUES inline data is
# transpilable today; the server-owned transpiled buffer is format-agnostic and would carry FORMAT
# data too, but no bundled dialect can currently produce such a query (and even a hypothetical identity
# transpiler drops the raw FORMAT payload rather than re-emitting it).
$CLICKHOUSE_CLIENT $POLY -q "INSERT INTO t FORMAT CSV
1
2
3" 2>&1 | grep -om1 "SYNTAX_ERROR"
echo "--- no insert from rejected FORMAT insert (expect: 100 5) ---"
$CLICKHOUSE_CLIENT -q "SELECT sum(x), count() FROM t"

# A polyglot inline INSERT is transpiled as a whole, so the inline data counts towards
# max_query_size (unlike a native ClickHouse INSERT, whose inline data is streamed and is not bounded
# by max_query_size). An oversized payload is rejected up front with a dedicated, actionable error
# instead of silently changing the INSERT size contract, and nothing is inserted.
big_values=$(seq 1 100 | sed 's/.*/(&)/' | paste -sd, -)
$CLICKHOUSE_CLIENT $POLY --max_query_size 100 -q "INSERT INTO t VALUES $big_values" 2>&1 | grep -om1 "counts towards max_query_size"
echo "--- no insert when payload exceeds max_query_size (expect: 100 5) ---"
$CLICKHOUSE_CLIENT -q "SELECT sum(x), count() FROM t"

# The size guard must also hold in --multiquery / script mode, where the client parses with
# allow_multi_statements enabled (which disables the generic per-query length limit): the client
# itself rejects the oversized statement before transpiling or sending anything. If the rejection
# came from the server instead, query_log would record the query_id as ExceptionBeforeStart.
oversized_id="${CLICKHOUSE_DATABASE}_04512_oversized"
$CLICKHOUSE_CLIENT $POLY --multiquery --max_query_size 100 --query_id="$oversized_id" -q "INSERT INTO t VALUES $big_values" 2>&1 | grep -om1 "counts towards max_query_size"
$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
echo "--- oversized query in multiquery mode is rejected on the client, without a server round trip (expect: 0) ---"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.query_log WHERE query_id = '$oversized_id'"

$CLICKHOUSE_CLIENT -q "DROP TABLE t"
$CLICKHOUSE_CLIENT -q "DROP TABLE b"
