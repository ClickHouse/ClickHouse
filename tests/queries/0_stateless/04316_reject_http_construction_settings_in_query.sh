#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The query-construction settings `select`/`filter`/`order`/`sort`/`page` are applied by the engine
# on the parsed query, so they are first-class on every protocol — including via an in-query
# `SETTINGS` clause on the native protocol, not only via the HTTP URL.

echo "--- filter wraps the query ---"
${CLICKHOUSE_CLIENT} --query "SELECT number FROM numbers(5) SETTINGS filter = 'number > 2'"
echo "--- select + order compose ---"
${CLICKHOUSE_CLIENT} --query "SELECT number, number * 10 AS x FROM numbers(5) SETTINGS \`select\` = 'x', order = 'x DESC'"
echo "--- sort with +/- prefixes ---"
${CLICKHOUSE_CLIENT} --query "SELECT number FROM numbers(5) SETTINGS sort = '-number'"
echo "--- page over limit ---"
${CLICKHOUSE_CLIENT} --query "SELECT number FROM numbers(10) SETTINGS limit = 2, page = 2"
echo "--- sort and order together is rejected ---"
${CLICKHOUSE_CLIENT} --query "SELECT 1 SETTINGS sort = 'a', order = 'b'" 2>&1 | grep -o -m1 "cannot be specified together"

# `compression` shapes the HTTP response body and is consumed before execution, so it has no effect
# via an in-query SETTINGS clause and is rejected with a clear error.
echo "--- compression in an in-query SETTINGS clause is rejected ---"
${CLICKHOUSE_CLIENT} --query "SELECT 1 SETTINGS compression = 'gz'" 2>&1 | grep -o -m1 "shapes the HTTP response body"
