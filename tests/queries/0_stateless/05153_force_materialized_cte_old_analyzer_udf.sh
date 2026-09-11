#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# On the old analyzer the guard runs twice: in `InterpreterSelectQuery` before the CTE rewrite, and in
# `TreeRewriter::normalize` after SQL UDF expansion, where a CTE hidden in a UDF body first becomes visible.
# The function name is per-database: UDF names are server-global and the flaky check runs this test
# concurrently with itself.

F="${CLICKHOUSE_DATABASE}_f"
OLD_ANALYZER="--enable_analyzer 0 --force_materialized_cte 1"

echo "-- materialized CTE from a SQL UDF body is rejected"
${CLICKHOUSE_CLIENT} -q "CREATE FUNCTION $F AS () -> (WITH c AS MATERIALIZED (SELECT number AS x FROM numbers(3)) SELECT count() FROM c AS a, c AS b)"
${CLICKHOUSE_CLIENT} ${OLD_ANALYZER} -q "SELECT $F()" 2>&1 | grep -oF 'SUPPORT_IS_DISABLED' | head -n 1
${CLICKHOUSE_CLIENT} -q "DROP FUNCTION $F"

echo "-- rejected before the CTE body is resolved"
# The early check must win over the errors that resolving the CTE body would raise.
${CLICKHOUSE_CLIENT} ${OLD_ANALYZER} -q "WITH c AS MATERIALIZED (SELECT x FROM no_such_table) SELECT count() FROM c AS a, c AS b" 2>&1 | grep -oF 'SUPPORT_IS_DISABLED' | head -n 1
