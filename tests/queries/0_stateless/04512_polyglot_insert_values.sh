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

# system.query_log stores the query as the user submitted it (for `query`/`normalized_query_hash`),
# not the ClickHouse SQL produced by the transpiler.
query_id="${CLICKHOUSE_DATABASE}_04512_log"
$CLICKHOUSE_CLIENT $POLY --query_id="$query_id" -q "INSERT INTO b VALUES (true)"
$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
echo "--- query_log keeps the original, untranspiled query text (expect: INSERT INTO b VALUES (true)) ---"
$CLICKHOUSE_CLIENT -q "SELECT query FROM system.query_log WHERE query_id = '$query_id' AND type = 'QueryStart' AND current_database = currentDatabase()"

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

$CLICKHOUSE_CLIENT -q "DROP TABLE t"
$CLICKHOUSE_CLIENT -q "DROP TABLE b"
