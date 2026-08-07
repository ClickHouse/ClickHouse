#!/usr/bin/env bash
# Tags: long, no-parallel
# shellcheck disable=SC2015

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

echo "A session successfully closes when timeout first expires with refcount != 1"
# Here we do not want an infinite loop - because we want this mechanism to be reliable in all cases
# So it's better to give it enough time to complete even in constrained environments
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&session_id=${CLICKHOUSE_DATABASE}_10&session_timeout=1" --data-binary "CREATE TEMPORARY TABLE x (n UInt64) AS SELECT number FROM numbers(10)"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&session_id=${CLICKHOUSE_DATABASE}_10&session_timeout=1" --data-binary "SELECT sum(n + sleep(3)) FROM x" # This query ensures timeout expires with refcount > 1
sleep 15
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&session_id=${CLICKHOUSE_DATABASE}_10&session_check=1" --data-binary "SELECT 1" | grep -c -F 'SESSION_NOT_FOUND'
