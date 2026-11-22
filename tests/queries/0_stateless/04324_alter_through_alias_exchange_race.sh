#!/usr/bin/env bash
# Tags: race

set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CLICKHOUSE_CLIENT="${CLICKHOUSE_CLIENT} --allow_experimental_alias_table_engine 1"

# ALTER through an Alias forwards the metadata change to the target table. The forwarded ALTER must
# serialize with EXCHANGE TABLES of the target, otherwise a concurrent EXCHANGE can slip in between
# resolving the target and IDatabase::alterTable and the ALTER throws CANNOT_ASSIGN_ALTER or UNKNOWN_TABLE.
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t1_04324 (x UInt64) ENGINE = MergeTree ORDER BY x"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t2_04324 (x UInt64) ENGINE = MergeTree ORDER BY x"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE a_04324 ENGINE = Alias('t1_04324')"

TIMEOUT=30

function thread_exchange()
{
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]; do
        ${CLICKHOUSE_CLIENT} -q "EXCHANGE TABLES t1_04324 AND t2_04324" 2>/dev/null ||:
    done
}

function thread_alter()
{
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    local i=0
    while [ $SECONDS -lt "$TIMELIMIT" ]; do
        i=$((i + 1))
        # No exceptions should reach the client: the target's DDLGuard taken in StorageAlias::alter
        # serializes the forwarded ALTER with EXCHANGE TABLES of the target.
        ${CLICKHOUSE_CLIENT} -q "ALTER TABLE a_04324 COMMENT COLUMN x 'c${i}'" 2>&1 | grep -Fa "Exception: " ||:
    done
}

thread_exchange &
thread_exchange &
thread_alter &
thread_alter &

wait

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS a_04324 SYNC"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t1_04324 SYNC"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t2_04324 SYNC"

echo "OK"
