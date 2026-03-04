#!/usr/bin/env bash

# Tags: long, no-fasttest, no-replicated-database, no-ordinary-database, no-encrypted-storage, no-azure-blob-storage

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./transactions.lib
. "$CURDIR"/transactions.lib

set -e

function cleanup
{
    ${CLICKHOUSE_CLIENT} --query="SYSTEM DISABLE FAILPOINT storage_merge_tree_delay_truncate" 2>/dev/null || true
    ${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS tab"
}

trap cleanup EXIT

${CLICKHOUSE_CLIENT} --query="CREATE TABLE tab (x INT) ENGINE=MergeTree ORDER BY x"

${CLICKHOUSE_CLIENT} --query="INSERT INTO tab VALUES (1)"
${CLICKHOUSE_CLIENT} --query="INSERT INTO tab VALUES (2)"

function truncate_table
{
    ${CLICKHOUSE_CLIENT} --query="SYSTEM ENABLE FAILPOINT storage_merge_tree_delay_truncate"
    ${CLICKHOUSE_CLIENT} --query="TRUNCATE TABLE tab"
}

tx 1 "BEGIN TRANSACTION"
tx 1 "OPTIMIZE TABLE tab"

# Start TRUNCATE in background -- the failpoint will delay it by 10 seconds after
# it captures the parts list, creating a window for the concurrent transaction below.
truncate_table &

# Give TRUNCATE time to start and capture the parts list before sleeping.
sleep 2

tx 1 "ROLLBACK"

${CLICKHOUSE_CLIENT} --query="SELECT _part, * FROM tab"

wait