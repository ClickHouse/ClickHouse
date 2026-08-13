#!/usr/bin/env bash
# Tags: long

set -e

CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=none

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS mt_00763_1"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS buffer_00763_1"

${CLICKHOUSE_CLIENT} --query="CREATE TABLE buffer_00763_1 (s String) ENGINE = Buffer($CLICKHOUSE_DATABASE, mt_00763_1, 1, 1, 1, 1, 1, 1, 1)"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE mt_00763_1 (x UInt32, s String) ENGINE = MergeTree ORDER BY x"
${CLICKHOUSE_CLIENT} --query="INSERT INTO mt_00763_1 VALUES (1, '1'), (2, '2'), (3, '3')"

function thread_alter()
{
    local TIMELIMIT=$((SECONDS+$1))
    local it=0
    while [ $SECONDS -lt "$TIMELIMIT" ] && [ $it -lt 300 ];
    do
        it=$((it+1))
        $CLICKHOUSE_CLIENT --ignore-error -q "
            ALTER TABLE mt_00763_1 MODIFY column s UInt32;
            ALTER TABLE mt_00763_1 MODIFY column s String;
        " ||:
    done
}

function thread_query()
{
    local TIMELIMIT=$((SECONDS+$1))
    local it=0
    while [ $SECONDS -lt "$TIMELIMIT" ] && [ $it -lt 2000 ];
    do
        it=$((it+1))
        $CLICKHOUSE_CLIENT --ignore-error -q "
            SELECT sum(length(s)) FROM buffer_00763_1;
        " 2>&1 | grep -vP '(^3$|^Received exception from server|^Code: 473)'
    done
}

export -f thread_alter
export -f thread_query

TIMEOUT=30
thread_alter $TIMEOUT &
thread_query $TIMEOUT &

wait

${CLICKHOUSE_CLIENT} --query="DROP TABLE mt_00763_1"
${CLICKHOUSE_CLIENT} --query="DROP TABLE buffer_00763_1"
