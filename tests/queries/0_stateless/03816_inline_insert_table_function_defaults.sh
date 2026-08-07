#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# When the server parses INSERT ... VALUES inline data itself
# (send_table_structure_on_insert_with_inline_data = 0, plus async_insert = 0 so the
# synchronous path is taken), an explicit NULL inserted into a non-Nullable column that
# has a DEFAULT must be replaced by the default, exactly as for a plain table INSERT and
# over HTTP (input_format_null_as_default, applied during parsing). Previously the
# server-side inline path skipped this for INSERT INTO TABLE FUNCTION, because the
# destination table_id is empty for table functions, so the NULL became 0 instead of the
# default.

# async_insert 0: the synchronous inline path (the buggy one) is taken.
# send_table_structure_on_insert_with_inline_data 0: the server parses the inline data.
# engine_file_truncate_on_insert 1: makes the file() cases idempotent across --test-runs.
OPT="--async_insert 0 --send_table_structure_on_insert_with_inline_data 0"
FOPT="$OPT --engine_file_truncate_on_insert 1"

echo '-- remote() table function: explicit NULL -> DEFAULT, matches plain table'
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_def"
$CLICKHOUSE_CLIENT --query "CREATE TABLE t_def (c Int DEFAULT 7) ENGINE = MergeTree ORDER BY tuple()"
$CLICKHOUSE_CLIENT $OPT --query "INSERT INTO FUNCTION remote('127.0.0.1:${CLICKHOUSE_PORT_TCP}', currentDatabase(), 't_def') VALUES (NULL)"
$CLICKHOUSE_CLIENT $OPT --query "INSERT INTO TABLE t_def VALUES (NULL)"
$CLICKHOUSE_CLIENT --query "SELECT c FROM t_def ORDER BY ALL"
$CLICKHOUSE_CLIENT --query "DROP TABLE t_def"

echo '-- file() CSV: explicit NULL -> DEFAULT (Int)'
$CLICKHOUSE_CLIENT $FOPT --query "INSERT INTO FUNCTION file('${CLICKHOUSE_DATABASE}_int.csv', CSV, 'a Int, b Int DEFAULT 77') VALUES (4, NULL)"
$CLICKHOUSE_CLIENT --query "SELECT a, b FROM file('${CLICKHOUSE_DATABASE}_int.csv', CSV, 'a Int, b Int DEFAULT 77') ORDER BY ALL"

echo '-- file() CSV: Nullable column keeps explicit NULL (null_as_default does not force default)'
$CLICKHOUSE_CLIENT $FOPT --query "INSERT INTO FUNCTION file('${CLICKHOUSE_DATABASE}_nullable.csv', CSV, 'a Int, b Nullable(Int) DEFAULT 77') VALUES (4, NULL)"
$CLICKHOUSE_CLIENT --query "SELECT a, b FROM file('${CLICKHOUSE_DATABASE}_nullable.csv', CSV, 'a Int, b Nullable(Int) DEFAULT 77') ORDER BY ALL"

echo '-- file() CSV: explicit NULL -> DEFAULT (LowCardinality(String))'
$CLICKHOUSE_CLIENT $FOPT --query "INSERT INTO FUNCTION file('${CLICKHOUSE_DATABASE}_lc.csv', CSV, 'a Int, s LowCardinality(String) DEFAULT ''x''') VALUES (1, NULL)"
$CLICKHOUSE_CLIENT --query "SELECT a, s FROM file('${CLICKHOUSE_DATABASE}_lc.csv', CSV, 'a Int, s LowCardinality(String) DEFAULT ''x''') ORDER BY ALL"

echo '-- file() CSV: explicit NULL -> DEFAULT (Array(Int))'
$CLICKHOUSE_CLIENT $FOPT --query "INSERT INTO FUNCTION file('${CLICKHOUSE_DATABASE}_arr.csv', CSV, 'a Int, arr Array(Int) DEFAULT [1, 2]') VALUES (1, NULL)"
$CLICKHOUSE_CLIENT --query "SELECT a, arr FROM file('${CLICKHOUSE_DATABASE}_arr.csv', CSV, 'a Int, arr Array(Int) DEFAULT [1, 2]') ORDER BY ALL"
