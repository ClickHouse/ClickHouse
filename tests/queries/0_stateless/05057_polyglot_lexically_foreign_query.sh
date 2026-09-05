#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: polyglot requires Rust build

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

POLY="--allow_experimental_polyglot_dialect 1 --dialect polyglot --polyglot_dialect bigquery"

# A source-dialect statement whose original text is not lexically valid ClickHouse SQL: BigQuery
# spells a single-line comment `#comment`, while the ClickHouse lexer only treats `#` as a comment
# when it is followed by a space or `!`, and returns an error token otherwise.
FOREIGN="SELECT 1 AS x #comment"

echo "--- plain ClickHouse dialect rejects it lexically (expect: SYNTAX_ERROR) ---"
$CLICKHOUSE_CLIENT -q "$FOREIGN" 2>&1 | grep -om1 "SYNTAX_ERROR"

# The native client must not run the ClickHouse lexer over the foreign text before handing it to
# the transpiler: the classifying parse happens on the transpiled ClickHouse SQL, so the client,
# `clickhouse-local` and the server all accept exactly the same set of statements.
echo "--- native client accepts it in the polyglot dialect (expect: 1) ---"
$CLICKHOUSE_CLIENT $POLY -q "$FOREIGN"

echo "--- clickhouse-local accepts it in the polyglot dialect (expect: 1) ---"
$CLICKHOUSE_LOCAL $POLY -q "$FOREIGN"

echo "--- the HTTP (server) interface accepts it too (expect: 1) ---"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&allow_experimental_polyglot_dialect=1&dialect=polyglot&polyglot_dialect=bigquery" -d "$FOREIGN"

# The same statement in a multi-statement script: the client splits and classifies every statement
# without lexing the foreign text either.
echo "--- multi-statement script (expect: 1 then 2) ---"
$CLICKHOUSE_CLIENT $POLY --queries-file /dev/stdin <<'EOF'
SELECT 1 AS x #first
EOF
$CLICKHOUSE_LOCAL $POLY -q "SELECT 2 AS y #second"

# An inline INSERT written in the same lexically foreign way still lands in the table: the whole
# statement, comment included, is transpiled server-side and the data is read from the transpiled
# query.
$CLICKHOUSE_CLIENT -q "CREATE TABLE t (x Int32) ENGINE = MergeTree ORDER BY x"
$CLICKHOUSE_CLIENT $POLY -q "INSERT INTO t VALUES (1), (2) #trailing comment"
echo "--- inline INSERT with a foreign comment (expect: 3 2) ---"
$CLICKHOUSE_CLIENT -q "SELECT sum(x), count() FROM t"
$CLICKHOUSE_CLIENT -q "DROP TABLE t"
