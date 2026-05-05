#!/usr/bin/env bash
# Tags: no-parallel
# Smoke test for SYSTEM ENABLE / WAIT / NOTIFY / DISABLE FAILPOINT coordination.
# Uses drop_database_before_exclusive_ddl_lock (PAUSEABLE_ONCE): fires inside
# DROP DATABASE after tables are processed but before the exclusive DDL lock.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FP="drop_database_before_exclusive_ddl_lock"
DROP_DB="${CLICKHOUSE_DATABASE}_fp_coord"

function cleanup()
{
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT ${FP}" 2>/dev/null || true
    $CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS ${DROP_DB}" 2>/dev/null || true
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT -q "CREATE DATABASE ${DROP_DB}"
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${DROP_DB}.t (n UInt64) ENGINE = Memory"

$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT ${FP}"

# DROP DATABASE will pause at the failpoint; run it in the background.
$CLICKHOUSE_CLIENT -q "DROP DATABASE ${DROP_DB}" &
DROP_PID=$!

# Wait until the DROP thread has paused.
$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT ${FP} PAUSE"
echo "paused"

# Resume and let DROP finish.
$CLICKHOUSE_CLIENT -q "SYSTEM NOTIFY FAILPOINT ${FP}"
wait $DROP_PID
echo "done"
