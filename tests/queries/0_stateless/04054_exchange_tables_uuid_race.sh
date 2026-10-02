#!/usr/bin/env bash
# Tags: race

set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB1="${CLICKHOUSE_DATABASE}_db1_04054"
DB2="${CLICKHOUSE_DATABASE}_db2_04054"

${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS ${DB1} SYNC"
${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS ${DB2} SYNC"
${CLICKHOUSE_CLIENT} -q "CREATE DATABASE ${DB1} ENGINE = Atomic"
${CLICKHOUSE_CLIENT} -q "CREATE DATABASE ${DB2} ENGINE = Atomic"

# Plain MergeTree + pure comment ALTER takes the short synchronous path in StorageMergeTree::alter
# (isCommentAlter branch), where the exception — if any — propagates back to the client. Before
# the DDLGuard fix, concurrent EXCHANGE TABLES could cause ALTER to observe the swapped metadata
# file and throw UNKNOWN_TABLE ("metadata file ... has different UUID") or CANNOT_ASSIGN_ALTER.
${CLICKHOUSE_CLIENT} -q "CREATE TABLE ${DB1}.t (x UInt64) ENGINE = MergeTree ORDER BY x"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE ${DB2}.t (x UInt64) ENGINE = MergeTree ORDER BY x"

TIMEOUT=30

function thread_exchange()
{
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]; do
        ${CLICKHOUSE_CLIENT} -q "EXCHANGE TABLES ${DB1}.t AND ${DB2}.t" 2>/dev/null ||:
    done
}

function thread_alter()
{
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    local i=0
    while [ $SECONDS -lt "$TIMELIMIT" ]; do
        i=$((i + 1))
        # No exceptions should reach the client here: the DDLGuard taken in InterpreterAlterQuery
        # serializes ALTER with EXCHANGE TABLES, so neither UNKNOWN_TABLE nor CANNOT_ASSIGN_ALTER
        # nor ATOMIC_RENAME_FAIL should surface.
        ${CLICKHOUSE_CLIENT} -q "ALTER TABLE ${DB1}.t COMMENT COLUMN x 'c${i}'" 2>&1 | grep -Fa "Exception: " ||:
        ${CLICKHOUSE_CLIENT} -q "ALTER TABLE ${DB2}.t COMMENT COLUMN x 'c${i}'" 2>&1 | grep -Fa "Exception: " ||:
    done
}

thread_exchange &
thread_exchange &
thread_exchange &
thread_alter &
thread_alter &
thread_alter &

wait

${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS ${DB1} SYNC"
${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS ${DB2} SYNC"

echo "OK"
