#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: polyglot requires Rust build

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The client-side half of the inline INSERT coverage (the native protocol, the rejected forms and
# the max_query_size guard) is in 04512_polyglot_insert_values; this test covers the HTTP
# interface, the pinning of parse-time settings for the server-side reparse and the streaming of
# external data. They are separate files so that neither exceeds the per-run time limit of the
# flaky check.

POLY="--allow_experimental_polyglot_dialect 1 --dialect polyglot --polyglot_dialect postgresql"
POLY_CH="--allow_experimental_polyglot_dialect 1 --dialect polyglot --polyglot_dialect clickhouse"

$CLICKHOUSE_CLIENT -q "CREATE TABLE t (x Int32) ENGINE = MergeTree ORDER BY x"

# Over the HTTP interface a streaming INSERT (`input_format_connection_handling` +
# `input_format_max_block_wait_ms`) keeps the request
# body separate from the URL `query` parameter, so the body is available to the INSERT as external
# data. For a foreign dialect that would mix two parsing rules in a single INSERT: the inline data
# is transpiled and counted towards `max_query_size`, while the body is neither. The server rejects
# a non-empty body instead of reading it, mirroring the client-side rule for stdin and INFILE.
poly_url="${CLICKHOUSE_URL}&allow_experimental_polyglot_dialect=1&dialect=polyglot&polyglot_dialect=postgresql&input_format_connection_handling=1&input_format_max_block_wait_ms=1000"

# An empty body is the normal way to run a polyglot INSERT over HTTP and must keep working.
${CLICKHOUSE_CURL} -sS -X POST "${poly_url}&query=INSERT%20INTO%20t%20VALUES%20(7)" -d ''
echo "--- HTTP polyglot inline INSERT with an empty body (expect: 7 1) ---"
$CLICKHOUSE_CLIENT -q "SELECT sum(x), count() FROM t"

${CLICKHOUSE_CURL} -sS -X POST "${poly_url}&query=INSERT%20INTO%20t%20VALUES%20(8)" -d '(9)' 2>&1 | grep -om1 "NOT_IMPLEMENTED"
echo "--- no insert when an HTTP body accompanies a polyglot INSERT (expect: 7 1) ---"
$CLICKHOUSE_CLIENT -q "SELECT sum(x), count() FROM t"

# A leading CTE does not change that this is an `INSERT`: keep its body separate from the URL query
# so it is rejected as external data instead of becoming extra inline VALUES rows.
${CLICKHOUSE_CURL} -sS -X POST "${poly_url}&query=WITH%20cte%20AS%20(SELECT%201)%20INSERT%20INTO%20t%20VALUES%20(10)" -d ',(11)' 2>&1 | grep -om1 "NOT_IMPLEMENTED"
echo "--- no insert when an HTTP body accompanies a CTE-wrapped polyglot INSERT (expect: 7 1) ---"
$CLICKHOUSE_CLIENT -q "SELECT sum(x), count() FROM t"

# The bareword `INSERT` on its own does not make a query an `INSERT` statement - it is also a legal
# identifier. Such a query must keep the legacy behavior of continuing the URL query with the
# request body instead of taking the streaming-safe path meant for inline data.
echo "--- the request body continues a URL query that only mentions the identifier insert ---"
${CLICKHOUSE_CURL} -sS -X POST "${CLICKHOUSE_URL}&input_format_max_block_wait_ms=1000&query=WITH%20cte%20AS%20(SELECT%201)%20SELECT%20instr('abcd'%2C'b')%20AS%20insert" -d ' FORMAT JSONEachRow'

# The scan must stop at the beginning of the statement that follows the CTE list. Looking for an
# `INSERT INTO` token pair anywhere in the statement would also match a `SELECT` that happens to use
# `insert` and `into` as column aliases, and its request body would stop being concatenated.
echo "--- the request body continues a URL query aliasing both insert and into ---"
${CLICKHOUSE_CURL} -sS -X POST "${CLICKHOUSE_URL}&input_format_max_block_wait_ms=1000&query=WITH%20cte%20AS%20(SELECT%201)%20SELECT%201%20AS%20insert%2C%202%20AS%20into" -d ' FORMAT JSONEachRow'

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
printf '5\n' | $CLICKHOUSE_CLIENT $POLY_CH -q "INSERT INTO t SELECT * FROM input('x Int32') FORMAT TSV"
echo "--- polyglot INSERT SELECT FROM input() streams stdin (expect: 12 2) ---"
$CLICKHOUSE_CLIENT -q "SELECT sum(x), count() FROM t"

# Without any external data the same query must fail loudly rather than hang waiting for it.
$CLICKHOUSE_CLIENT $POLY_CH -q "INSERT INTO t SELECT * FROM input('x Int32') FORMAT TSV" < /dev/null 2>&1 | grep -om1 "NO_DATA_TO_INSERT"
echo "--- no insert from an input() query without data (expect: 12 2) ---"
$CLICKHOUSE_CLIENT -q "SELECT sum(x), count() FROM t"

# What a rejected polyglot INSERT leaves in the logs (the exception-before-start redaction, incl.
# CTE-wrapped and EXPLAIN-wrapped forms and raw FORMAT payloads) is covered by
# 04843_polyglot_insert_log_redaction.

# `EXPLAIN INSERT` in a foreign dialect is not transpilable: the bundled dialects reject `EXPLAIN` in
# front of an `INSERT`, and the `clickhouse` source dialect rewrites `EXPLAIN` into `DESCRIBE`, which
# ClickHouse does not accept in front of an `INSERT`. It must fail cleanly, without inserting.
$CLICKHOUSE_CLIENT $POLY -q "EXPLAIN INSERT INTO t VALUES (987654324)" 2>&1 | grep -om1 "SYNTAX_ERROR"
echo "--- no insert from a polyglot EXPLAIN INSERT (expect: 12 2) ---"
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
echo "--- inline data survives a dialect switch in the INSERT's own SETTINGS clause (expect: 25 3) ---"
$CLICKHOUSE_CLIENT -q "SELECT sum(x), count() FROM t"

# The pinning covers every parse-time setting the classifier consumed, not only the dialect: the
# server reparses the verbatim text under the same parser settings the client parsed it with. Here
# the query's own SETTINGS clause shrinks the parse limits; without the pin the server would reparse
# the very same text the client already accepted with `max_parser_depth = 1` (or reject it against
# `max_query_size = 1`) and fail, even though the settings only apply to the query's execution.
echo "--- parse limits are pinned for the server-side reparse (expect: 7, 42) ---"
$CLICKHOUSE_CLIENT $POLY_CH -q "SELECT 3 + 4 SETTINGS max_parser_depth = 1"
$CLICKHOUSE_CLIENT $POLY_CH -q "SELECT 41 + 1 SETTINGS max_query_size = 1"

# A client may also ask for the HTTP 100 Continue response to be deferred until after the quota
# checks. The request body cannot be read before that response is sent, so the presence of external
# data is decided from the request headers. A deferred `100 Continue` without a body carries no
# external data and must keep working (see 03353_http_100_continue), while a body must still be
# rejected the same way as without the deferral.
defer_headers=(-H "Expect: 100-continue" -H "X-ClickHouse-100-Continue: defer" --expect100-timeout 300 --max-time 60)

${CLICKHOUSE_CURL} -sS -X POST "${poly_url}&query=INSERT%20INTO%20t%20VALUES%20(14)" "${defer_headers[@]}" -d ''
echo "--- deferred HTTP 100 Continue without a body still inserts (expect: 39 4) ---"
$CLICKHOUSE_CLIENT -q "SELECT sum(x), count() FROM t"

# `-T -` uploads from stdin with an unknown size, which is what makes `curl` chunk-encode the
# request and emit the terminating chunk. Setting `Transfer-Encoding: chunked` by hand instead only
# relabels a request whose body `curl` still sends unencoded, and older `curl` then never sends that
# terminating chunk, so the server waits for a body that never arrives.
${CLICKHOUSE_CURL} -sS -X POST "${poly_url}&query=INSERT%20INTO%20t%20VALUES%20(15)" "${defer_headers[@]}" -T - < /dev/null
echo "--- deferred chunked HTTP 100 Continue without a body still inserts (expect: 54 5) ---"
$CLICKHOUSE_CLIENT -q "SELECT sum(x), count() FROM t"

${CLICKHOUSE_CURL} -sS -X POST "${poly_url}&query=INSERT%20INTO%20t%20VALUES%20(15)" "${defer_headers[@]}" -d '(16)' 2>&1 | grep -om1 "NOT_IMPLEMENTED"
echo "--- deferred HTTP 100 Continue with a body is still rejected (expect: 54 5) ---"
$CLICKHOUSE_CLIENT -q "SELECT sum(x), count() FROM t"

# A foreign-dialect INSERT that carries no inline data at all keeps the ordinary native streaming
# path: the transpiled query has no data section either, so the client streams stdin (or INFILE) in
# data packets exactly as for a native INSERT, and the server does not reject external data for it.
# Only the `clickhouse` source dialect is used here, because the bundled foreign dialects do not
# parse the `FORMAT` clause.
printf '17\n' | $CLICKHOUSE_CLIENT $POLY_CH -q "INSERT INTO t FORMAT TSV"
echo "--- polyglot INSERT without inline data streams stdin (expect: 71 6) ---"
$CLICKHOUSE_CLIENT -q "SELECT sum(x), count() FROM t"

# ... while stdin data on top of an INSERT that does carry its data inline is still rejected.
printf '18\n' | $CLICKHOUSE_CLIENT $POLY_CH -q "INSERT INTO t VALUES (19)" 2>&1 | grep -om1 "NOT_IMPLEMENTED"
echo "--- inline data plus stdin is still rejected (expect: 71 6) ---"
$CLICKHOUSE_CLIENT -q "SELECT sum(x), count() FROM t"

$CLICKHOUSE_CLIENT -q "DROP TABLE t"
