#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Valid ARRAY JOIN expression lists keep working.
$CLICKHOUSE_CLIENT -q "SELECT a, b FROM system.one ARRAY JOIN [1, 2] AS a, [3, 4] AS b ORDER BY a, b"
$CLICKHOUSE_CLIENT -q "SELECT a FROM system.one LEFT ARRAY JOIN [1, 2] AS a ORDER BY a"
# A COLUMNS matcher is itself an expression, so the list is not empty and still parses; an empty
# match is reported after resolution.
$CLICKHOUSE_CLIENT -q "SELECT 1 FROM system.one ARRAY JOIN COLUMNS('nomatchxyz')" 2>&1 | grep -o -m1 "NUMBER_OF_ARGUMENTS_DOESNT_MATCH"

# An ARRAY JOIN with no expressions is not a valid clause, so it is rejected while parsing.
# Otherwise the formatter emits a dangling `ARRAY JOIN` keyword, which inside a set operation
# swallows the next branch's SELECT, so the format/parse round-trip diverges (this triggers the
# Inconsistent AST formatting consistency check in debug builds).
$CLICKHOUSE_CLIENT -q "SELECT x FROM (SELECT [1] AS x) ARRAY JOIN" 2>&1 | grep -o -m1 "SYNTAX_ERROR"
$CLICKHOUSE_CLIENT -q "(SELECT x FROM (SELECT [1] AS x) ARRAY JOIN) INTERSECT ALL (SELECT 1 AS y)" 2>&1 | grep -o -m1 "SYNTAX_ERROR"
$CLICKHOUSE_CLIENT -q "(SELECT x FROM (SELECT [1] AS x) LEFT ARRAY JOIN) UNION ALL (SELECT 1 AS y)" 2>&1 | grep -o -m1 "SYNTAX_ERROR"

# The JSON AST entry point does not go through the parser, so it rejects an empty expression list
# on its own. A valid ARRAY JOIN still round-trips through JSON unchanged.
$CLICKHOUSE_CLIENT -q "SELECT formatQueryFromJSON(parseQueryToJSON('SELECT a FROM system.one ARRAY JOIN [1] AS a'))"
$CLICKHOUSE_CLIENT -q "WITH parseQueryToJSON('SELECT a FROM system.one ARRAY JOIN [1] AS a') AS j SELECT position(j, '\"expression_list\":{\"type\":\"ExpressionList\",\"children\":[') > 0"
$CLICKHOUSE_CLIENT -q "WITH parseQueryToJSON('SELECT a FROM system.one ARRAY JOIN [1] AS a') AS j SELECT formatQueryFromJSON(replaceAll(j, '\"expression_list\":{\"type\":\"ExpressionList\",\"children\":[{\"type\":\"Literal\",\"value\":{\"field_type\":\"Array\",\"value\":[{\"field_type\":\"UInt64\",\"value\":1}]},\"alias\":\"a\"}]}', '\"expression_list\":{\"type\":\"ExpressionList\",\"children\":[]}'))" 2>&1 | grep -o -m1 "BAD_ARGUMENTS"
