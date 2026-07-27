#!/usr/bin/env bash
# Inline `SETTINGS allow_experimental_detach_queries = 0` must be able to opt a single query out of
# detached execution enabled through a URL parameter (or a profile). The HTTP handler consults only
# the effective setting value, so the usual settings precedence applies: the query text wins.
set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Detaching returns a `query_id` instead of the query result, so a numeric result proves the query
# ran synchronously and its rows were sent back to the client.
echo "=== URL enables detach, inline SETTINGS opts out ==="
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&allow_experimental_detach_queries=1&async_insert=0" \
    -X POST --data-binary "SELECT 42 SETTINGS allow_experimental_detach_queries = 0"

echo "=== URL enables detach, no inline opt-out: query is detached ==="
DETACHED=$(${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&allow_experimental_detach_queries=1&async_insert=0" \
    -X POST --data-binary "SELECT 42")
# The detached response carries a `query_id`, never the result row.
if [ "$DETACHED" = "42" ] || [ -z "$DETACHED" ]; then
    echo "FAIL: expected a query_id, got: '$DETACHED'"
    exit 1
fi
echo "<query_id>"

echo "=== Inline SETTINGS alone still enables detach ==="
DETACHED_INLINE=$(${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&async_insert=0" \
    -X POST --data-binary "SELECT 42 SETTINGS allow_experimental_detach_queries = 1")
if [ "$DETACHED_INLINE" = "42" ] || [ -z "$DETACHED_INLINE" ]; then
    echo "FAIL: expected a query_id, got: '$DETACHED_INLINE'"
    exit 1
fi
echo "<query_id>"
