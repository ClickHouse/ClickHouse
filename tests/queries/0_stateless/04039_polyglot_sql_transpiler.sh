#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: polyglot requires Rust build

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

POLYGLOT_OPTS="--allow_experimental_polyglot_dialect 1 --dialect polyglot"

# SQLite: TYPEOF() does not exist in ClickHouse
$CLICKHOUSE_CLIENT $POLYGLOT_OPTS --polyglot_dialect sqlite \
    -q "SELECT TYPEOF(42)"

# MySQL: double-quoted strings are string literals in MySQL but identifiers in ClickHouse
$CLICKHOUSE_CLIENT $POLYGLOT_OPTS --polyglot_dialect mysql \
    -q 'SELECT "hello world"'

# PostgreSQL: FETCH FIRST N ROWS ONLY is not supported by the ClickHouse parser
$CLICKHOUSE_CLIENT $POLYGLOT_OPTS --polyglot_dialect postgresql \
    -q "SELECT number FROM numbers(10) FETCH FIRST 3 ROWS ONLY"

# Snowflake: IFF() does not exist in ClickHouse (it uses if() instead)
$CLICKHOUSE_CLIENT $POLYGLOT_OPTS --polyglot_dialect snowflake \
    -q "SELECT IFF(1 > 0, 'yes', 'no')"

# DuckDB: SELECT * EXCLUDE(col) is not supported by the ClickHouse parser
$CLICKHOUSE_CLIENT $POLYGLOT_OPTS --polyglot_dialect duckdb \
    -q "SELECT * EXCLUDE(b) FROM (SELECT 1 AS a, 2 AS b, 3 AS c)"

# Test that polyglot dialect requires the experimental setting
$CLICKHOUSE_CLIENT --dialect polyglot --polyglot_dialect sqlite -q "SELECT 1" 2>&1 | grep -om1 "SUPPORT_IS_DISABLED"

# Test that an invalid dialect name produces a clear error
$CLICKHOUSE_CLIENT $POLYGLOT_OPTS --polyglot_dialect invalid_dialect \
    -q "SELECT 1" 2>&1 | grep -om1 'SYNTAX_ERROR'

# Test that an empty dialect name produces a clear error
$CLICKHOUSE_CLIENT $POLYGLOT_OPTS --polyglot_dialect '' \
    -q "SELECT 1" 2>&1 | grep -om1 'SYNTAX_ERROR'

# Test that multi-statement input is rejected
$CLICKHOUSE_CLIENT $POLYGLOT_OPTS --polyglot_dialect sqlite \
    -q "SELECT 1; SELECT 2" 2>&1 | grep -om1 'SYNTAX_ERROR'
