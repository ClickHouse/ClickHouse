#!/usr/bin/env bash
# Tags: no-asan, no-msan, no-tsan

# Such a huge timeout mostly for debug build.
CLICKHOUSE_CURL_TIMEOUT=60

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Too deep recursion
rep '(' 10000 | $CLICKHOUSE_CURL -sS "$CLICKHOUSE_URL" --data-binary @- | grep -cP 'exceeded|too large'
{ echo -n 'SELECT '; rep '[' 10000; } | $CLICKHOUSE_CURL -sS "$CLICKHOUSE_URL" --data-binary @- | grep -cP 'exceeded|too large'
{ echo -n 'SELECT '; rep '([' 5000; } | $CLICKHOUSE_CURL -sS "$CLICKHOUSE_URL" --data-binary @- | grep -cP 'exceeded|too large'
{ echo -n 'SELECT 1'; rep '+1' 10000; } | $CLICKHOUSE_CURL -sS "$CLICKHOUSE_URL" --data-binary @- | grep -cP 'exceeded|too large'

# But this is Ok
{ echo -n 'SELECT 1'; rep ',1' 10000; } | $CLICKHOUSE_CURL -sS "$CLICKHOUSE_URL" --data-binary @- | wc -c
{ echo -n 'SELECT 1'; rep ' OR 1' 10000; } | $CLICKHOUSE_CURL -sS "$CLICKHOUSE_URL" --data-binary @-
