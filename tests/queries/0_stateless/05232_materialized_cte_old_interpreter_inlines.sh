#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The old interpreter has no CTE name resolution of its own, so on the query paths that still reach
# it a `MATERIALIZED` CTE is inlined at every reference, as a plain one.
# https://github.com/ClickHouse/ClickHouse/issues/113711
# A shell test because the dummy alias of the `WITH` element's own subquery is numbered from a
# process-global counter (`subquery_index` in `QueryAliasesVisitor.cpp`), so it has to be masked.
# Reading the `EXPLAIN` from a subquery instead would analyze the inlined body copies one step
# further and no longer show the tree the old interpreter built.

${CLICKHOUSE_CLIENT} -nm -q "
SET enable_materialized_cte = 1;

DROP TABLE IF EXISTS t_old_113711b;
CREATE TABLE t_old_113711b (x UInt8) ENGINE = MergeTree ORDER BY x;

SELECT '-- EXPLAIN SYNTAX of a non-SELECT statement';
-- The line below documents the inlined form; on its own it does not tell the two apart, because an
-- inlined body prints under its alias, which is the CTE's name, exactly where a kept reference would
-- print that name (the text is byte-identical to the same statement with a plain CTE). What it guards
-- is that the statement runs at all: a kept reference reaches the old interpreter as an unknown table.
EXPLAIN SYNTAX INSERT INTO t_old_113711b WITH c_old_113711b AS MATERIALIZED (SELECT 1 AS x) SELECT x FROM c_old_113711b;

SELECT '-- EXPLAIN AST with optimize = 1';
-- This block is the proof of the inlining: the \`TableExpression\` holds a \`Subquery (alias
-- c_old_113711b)\`, a copy of the CTE's body, where a kept reference would leave an \`Identifier\`.
EXPLAIN AST optimize = 1 WITH c_old_113711b AS MATERIALIZED (SELECT 1 AS x) SELECT x FROM c_old_113711b;

DROP TABLE t_old_113711b;
" | sed -E 's/_subquery[0-9]+/_subqueryN/'
