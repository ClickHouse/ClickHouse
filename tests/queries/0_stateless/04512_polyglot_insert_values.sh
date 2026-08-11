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

# What the inline INSERT data must and must not leave in the query log — on the success path
# and on every failure path — is covered by 04843_polyglot_insert_log_redaction, kept separate so
# that neither test exceeds the per-run time limit of the flaky check.

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

# The same guard in --multiquery / script mode (where the client itself must reject the oversized
# statement before transpiling or sending anything) is covered by
# 04843_polyglot_insert_log_redaction, which proves the absence of a server round trip via query_log.

# Over the HTTP interface a streaming INSERT (`input_format_connection_handling` +
# `input_format_max_block_wait_ms`) keeps the request
# body separate from the URL `query` parameter, so the body is available to the INSERT as external
# data. For a foreign dialect that would mix two parsing rules in a single INSERT: the inline data
# is transpiled and counted towards `max_query_size`, while the body is neither. The server rejects
# a non-empty body instead of reading it, mirroring the client-side rule for stdin and INFILE.
poly_url="${CLICKHOUSE_URL}&allow_experimental_polyglot_dialect=1&dialect=polyglot&polyglot_dialect=postgresql&input_format_connection_handling=1&input_format_max_block_wait_ms=1000"

# An empty body is the normal way to run a polyglot INSERT over HTTP and must keep working.
${CLICKHOUSE_CURL} -sS -X POST "${poly_url}&query=INSERT%20INTO%20t%20VALUES%20(7)" -d ''
echo "--- HTTP polyglot inline INSERT with an empty body (expect: 107 6) ---"
$CLICKHOUSE_CLIENT -q "SELECT sum(x), count() FROM t"

${CLICKHOUSE_CURL} -sS -X POST "${poly_url}&query=INSERT%20INTO%20t%20VALUES%20(8)" -d '(9)' 2>&1 | grep -om1 "NOT_IMPLEMENTED"
echo "--- no insert when an HTTP body accompanies a polyglot INSERT (expect: 107 6) ---"
$CLICKHOUSE_CLIENT -q "SELECT sum(x), count() FROM t"

# The client parses the transpiled SQL only to classify the query, but it must do so with the same
# parser flags the server uses to execute it (see `executeQuery`). Otherwise a query the server
# accepts is rejected locally before it is ever sent: with `implicit_select`, a bare expression is
# a valid query, and the classifier has to accept it too.
echo "--- implicit_select is honoured by the client-side classifier (expect: 2) ---"
$CLICKHOUSE_CLIENT $POLY --implicit_select 1 -q "1 + 1"

# `INSERT ... SELECT * FROM input(...)` does not carry its data inline: the server asks for it
# explicitly, exactly as for a native INSERT. Sending the query verbatim must not take that away —
# the client still has to stream stdin for it. Only the `clickhouse` source dialect is used here,
# because the bundled foreign dialects do not parse the `FORMAT` clause.
POLY_CH="--allow_experimental_polyglot_dialect 1 --dialect polyglot --polyglot_dialect clickhouse"
printf '5\n' | $CLICKHOUSE_CLIENT $POLY_CH -q "INSERT INTO t SELECT * FROM input('x Int32') FORMAT TSV"
echo "--- polyglot INSERT SELECT FROM input() streams stdin (expect: 112 7) ---"
$CLICKHOUSE_CLIENT -q "SELECT sum(x), count() FROM t"

# Without any external data the same query must fail loudly rather than hang waiting for it.
$CLICKHOUSE_CLIENT $POLY_CH -q "INSERT INTO t SELECT * FROM input('x Int32') FORMAT TSV" < /dev/null 2>&1 | grep -om1 "NO_DATA_TO_INSERT"
echo "--- no insert from an input() query without data (expect: 112 7) ---"
$CLICKHOUSE_CLIENT -q "SELECT sum(x), count() FROM t"

# What a rejected polyglot INSERT leaves in the logs (the exception-before-start redaction, incl.
# CTE-wrapped and EXPLAIN-wrapped forms and raw FORMAT payloads) is covered by
# 04843_polyglot_insert_log_redaction.

# `EXPLAIN INSERT` in a foreign dialect is not transpilable: the bundled dialects reject `EXPLAIN` in
# front of an `INSERT`, and the `clickhouse` source dialect rewrites `EXPLAIN` into `DESCRIBE`, which
# ClickHouse does not accept in front of an `INSERT`. It must fail cleanly, without inserting.
$CLICKHOUSE_CLIENT $POLY -q "EXPLAIN INSERT INTO t VALUES (987654324)" 2>&1 | grep -om1 "SYNTAX_ERROR"
echo "--- no insert from a polyglot EXPLAIN INSERT (expect: 112 7) ---"
$CLICKHOUSE_CLIENT -q "SELECT sum(x), count() FROM t"

# The query's own SETTINGS clause must not change how the very same text is handled on the wire:
# the AST was classified under the parse-time polyglot settings, so those are pinned for this query.
# Disabling the polyglot dialect from inside the query must not make the server reject the text the
# client already accepted; the SETTINGS clause still applies to the query's execution itself.
echo "--- a polyglot query may disable the polyglot dialect in its own SETTINGS clause (expect: 42) ---"
$CLICKHOUSE_CLIENT $POLY_CH -q "SELECT 41 + 1 SETTINGS allow_experimental_polyglot_dialect = 0"

# Likewise, switching `dialect` inside an inline INSERT must not pull it off the verbatim path: the
# polyglot parser already cleared `insert->data` (the server reads the data from the transpiled
# text), so taking the native client path here would lose the inline data.
$CLICKHOUSE_CLIENT $POLY_CH -q "INSERT INTO t SETTINGS dialect = 'clickhouse' VALUES (13)"
echo "--- inline data survives a dialect switch in the INSERT's own SETTINGS clause (expect: 125 8) ---"
$CLICKHOUSE_CLIENT -q "SELECT sum(x), count() FROM t"

$CLICKHOUSE_CLIENT -q "DROP TABLE t"
$CLICKHOUSE_CLIENT -q "DROP TABLE b"
