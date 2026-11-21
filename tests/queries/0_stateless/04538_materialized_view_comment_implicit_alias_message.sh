#!/usr/bin/env bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Regression test: the duplicate-comment SYNTAX_ERROR for
# CREATE MATERIALIZED VIEW ... COMMENT 'pre' AS SELECT ... COMMENT 'post'
# must come from the dedicated duplicate-comment diagnostic, not merely from
# the trailing COMMENT token being left unconsumed (both produce SYNTAX_ERROR,
# so the error code alone does not distinguish them).
$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS src_04538_sh; DROP TABLE IF EXISTS dst_04538_sh;" 2>/dev/null
$CLICKHOUSE_CLIENT --query="CREATE TABLE src_04538_sh (x UInt8) ENGINE = Memory;"
$CLICKHOUSE_CLIENT --query="CREATE TABLE dst_04538_sh (x UInt8) ENGINE = Memory;"
$CLICKHOUSE_CLIENT --query="CREATE MATERIALIZED VIEW mv_04538_sh_dup (x UInt8) ENGINE = Memory COMMENT 'pre-as comment' AS SELECT x FROM src_04538_sh COMMENT 'post-select comment';" 2>&1 | grep -o 'Comment for a view cannot be specified both before and after AS SELECT; please use only one'
$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS src_04538_sh; DROP TABLE IF EXISTS dst_04538_sh;"

# Same check for ParserCreateWindowViewQuery.
$CLICKHOUSE_CLIENT --allow_experimental_window_view=1 --allow_experimental_analyzer=0 --query="DROP TABLE IF EXISTS src_wv_04538_sh;" 2>/dev/null
$CLICKHOUSE_CLIENT --allow_experimental_window_view=1 --allow_experimental_analyzer=0 --query="CREATE TABLE src_wv_04538_sh (x UInt8, ts DateTime) ENGINE = Memory;"
$CLICKHOUSE_CLIENT --allow_experimental_window_view=1 --allow_experimental_analyzer=0 --query="CREATE WINDOW VIEW wv_04538_sh_dup (x UInt8, w_start DateTime) ENGINE = Memory COMMENT 'pre-as comment' AS SELECT x, tumbleStart(w_id) AS w_start FROM src_wv_04538_sh GROUP BY x, tumble(ts, toIntervalSecond(5)) AS w_id COMMENT 'post-select comment';" 2>&1 | grep -o 'Comment for a view cannot be specified both before and after AS SELECT; please use only one'
$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS src_wv_04538_sh;"

# Same check for the CREATE TABLE ... AS SELECT form (ParserCreateTableQuery),
# since the COMMENT-as-implicit-alias lookahead fix is global to ParserAlias.
$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS src_tbl_04538_sh;" 2>/dev/null
$CLICKHOUSE_CLIENT --query="CREATE TABLE src_tbl_04538_sh (x UInt8) ENGINE = Memory;"
$CLICKHOUSE_CLIENT --query="CREATE TABLE t_04538_sh_dup ENGINE = Memory COMMENT 'pre-as comment' AS SELECT x FROM src_tbl_04538_sh COMMENT 'post-select comment';" 2>&1 | grep -o 'Comment for a table cannot be specified both before and after AS; please use only one'
$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS src_tbl_04538_sh;"

# Same check for the AS table form (not AS SELECT), since CREATE TABLE
# already supported a trailing comment there before this PR.
$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS base_04538_sh;" 2>/dev/null
$CLICKHOUSE_CLIENT --query="CREATE TABLE base_04538_sh (a Int32) ENGINE = TinyLog COMMENT 'original comment';"
$CLICKHOUSE_CLIENT --query="CREATE TABLE t_astable_dup_04538_sh COMMENT 'pre comment' AS base_04538_sh COMMENT 'post comment';" 2>&1 | grep -o 'Comment for a table cannot be specified both before and after AS; please use only one'
$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS base_04538_sh;"

# Same check for the AS table_function() form (e.g. AS numbers(5)), since
# CREATE TABLE already supported a trailing comment there before this PR.
$CLICKHOUSE_CLIENT --query="CREATE TABLE t_astf_dup_04538_sh COMMENT 'pre comment' AS numbers(5) COMMENT 'post comment';" 2>&1 | grep -o 'Comment for a table cannot be specified both before and after AS; please use only one'
