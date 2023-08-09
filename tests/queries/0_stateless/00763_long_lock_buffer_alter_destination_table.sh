#!/usr/bin/env bash
# Tags: long, no-parallel

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

function thread1()
{
    seq 1 300 | sed -r -e 's/.+/ALTER TABLE mt_00763_1 MODIFY column s UInt32; ALTER TABLE mt_00763_1 MODIFY column s String;/' | ${CLICKHOUSE_CLIENT} --multiquery --ignore-error ||:
}

function thread2()
{
    seq 1 2000 | sed -r -e 's/.+/SELECT sum(length(s)) FROM buffer_00763_1;/' | ${CLICKHOUSE_CLIENT} --multiquery --ignore-error 2>&1 | grep -vP '(^3$|^Received exception from server|^Code: 473)'
}

thread1 &
thread2 &

wait

${CLICKHOUSE_CLIENT} --query="DROP TABLE mt_00763_1"
${CLICKHOUSE_CLIENT} --query="DROP TABLE buffer_00763_1"
