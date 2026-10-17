#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: polyglot requires Rust build

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `INSERT ... SELECT * FROM input(...)` is the one INSERT shape whose data still travels outside the
# query text in a foreign SQL dialect: the statement itself carries no inline data, so the client
# sends it verbatim and then streams stdin as usual. In `clickhouse-local` the `input()` initializer
# reparses the query it was given (see `LocalConnection`), and that query is the *original*
# foreign-dialect text - so it has to be reparsed with the same dialect that accepted it, not with
# the plain ClickHouse parser.
# The format is given with the `input_format` setting rather than with the `--input-format` option on
# purpose: the option makes `clickhouse-local` create its implicit table for stdin, which is a
# different (ClickHouse SQL) statement that has nothing to do with this test.

DB_PATH="${CLICKHOUSE_TMP}/05227_polyglot_local_input_function"
rm -rf "${DB_PATH:?}"
mkdir -p "$DB_PATH"

$CLICKHOUSE_LOCAL --path "$DB_PATH" -q "CREATE TABLE t (x Int32) ENGINE = MergeTree ORDER BY x" < /dev/null

echo "--- PostgreSQL-style cast over input() (expect: 3 2) ---"
printf '1\n2\n' | $CLICKHOUSE_LOCAL --path "$DB_PATH" --allow_experimental_polyglot_dialect 1 --dialect polyglot --polyglot_dialect postgresql --input_format TSV \
    -q "INSERT INTO t SELECT x::Int32 FROM input('x String')"
$CLICKHOUSE_LOCAL --path "$DB_PATH" -q "SELECT sum(x), count() FROM t" < /dev/null

# A query that the ClickHouse parser cannot even tokenize: BigQuery spells a single-line comment
# `#comment`, while the ClickHouse lexer only treats `#` as a comment when it is followed by a space
# or `!`. If the `input()` initializer reparsed this text with the ClickHouse parser it would fail.
echo "--- lexically foreign query over input() (expect: 18 4) ---"
printf '7\n8\n' | $CLICKHOUSE_LOCAL --path "$DB_PATH" --allow_experimental_polyglot_dialect 1 --dialect polyglot --polyglot_dialect bigquery --input_format TSV \
    -q "INSERT INTO t SELECT x FROM input('x Int32') #comment"
$CLICKHOUSE_LOCAL --path "$DB_PATH" -q "SELECT sum(x), count() FROM t" < /dev/null

echo "--- the same text in the plain ClickHouse dialect (expect: SYNTAX_ERROR, table unchanged: 18 4) ---"
printf '9\n' | $CLICKHOUSE_LOCAL --path "$DB_PATH" --input_format TSV \
    -q "INSERT INTO t SELECT x FROM input('x Int32') #comment" 2>&1 | grep -om1 "SYNTAX_ERROR"
$CLICKHOUSE_LOCAL --path "$DB_PATH" -q "SELECT sum(x), count() FROM t" < /dev/null

rm -rf "${DB_PATH:?}"
