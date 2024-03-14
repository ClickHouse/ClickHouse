#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# (for now) simple tests for unsupported data type or result cannot be stored in a column
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS samt SYNC;" #2>&1 | grep -o -F 'Exception'

${CLICKHOUSE_CLIENT} --query "CREATE TABLE samt (k UInt32, uint64_val UInt64, int32_val Int32, nullable_str Nullable(String)) ENGINE = StatelessAggregatingMergeTree(anyLast) ORDER BY k;"
${CLICKHOUSE_CLIENT} --query "INSERT INTO samt VALUES (1,1,1,'qwe'),(1,2,2,'rty'),(2,1,1,NULL);"
${CLICKHOUSE_CLIENT} --query "INSERT INTO samt VALUES (1,1,1,'newl'),(2,1,1,'NonNull');"
${CLICKHOUSE_CLIENT} --query "INSERT INTO samt VALUES (2,2,2,NULL);"

${CLICKHOUSE_CLIENT} --query "optimize table samt;"
${CLICKHOUSE_CLIENT} --query "select * from samt order by k;"