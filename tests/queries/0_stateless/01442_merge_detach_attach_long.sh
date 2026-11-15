#!/usr/bin/env bash
# Tags: long, no-debug

set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=none
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS t"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE t (x Int8) ENGINE = MergeTree ORDER BY tuple()"

function thread_ops()
{
    local TIMELIMIT=$((SECONDS+$1))
    local it=0
    while [ $SECONDS -lt "$TIMELIMIT" ] && [ $it -lt 100 ];
    do
        it=$((it+1))
        ${CLICKHOUSE_CLIENT} --query="INSERT INTO t VALUES (0)"
        ${CLICKHOUSE_CLIENT} --query="INSERT INTO t VALUES (0)"
        ${CLICKHOUSE_CLIENT} --query="OPTIMIZE TABLE t FINAL" 2>/dev/null &
        ${CLICKHOUSE_CLIENT} --query="ALTER TABLE t DETACH PARTITION tuple()"
        ${CLICKHOUSE_CLIENT} --query="SELECT count() FROM t HAVING count() > 0"
    done
}
export -f thread_ops

TIMEOUT=30
thread_ops $TIMEOUT &
wait

$CLICKHOUSE_CLIENT -q "DROP TABLE t"
