#!/usr/bin/env bash
# Tags: long, no-ordinary-database

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS mt";
$CLICKHOUSE_CLIENT --query "CREATE TABLE mt (n Int64) ENGINE=MergeTree ORDER BY n SETTINGS old_parts_lifetime=0";


function begin_commit_readonly()
{
  while true; do
    $CLICKHOUSE_CLIENT --multiquery --query "
            SET wait_changes_become_visible_after_commit_mode='wait';
            BEGIN TRANSACTION;
            COMMIT;" 2>&1| grep -Fa "Exception: " | grep -Fv UNKNOWN_STATUS_OF_TRANSACTION
  done
}

function begin_rollback_readonly()
{
  while true; do
    $CLICKHOUSE_CLIENT --wait_changes_become_visible_after_commit_mode=wait_unknown --multiquery --query "
            BEGIN TRANSACTION;
            SET TRANSACTION SNAPSHOT 42;
            ROLLBACK;"
  done
}

function begin_insert_commit()
{
  while true; do
    $CLICKHOUSE_CLIENT --wait_changes_become_visible_after_commit_mode=async --multiquery --query "
            BEGIN TRANSACTION;
            INSERT INTO mt VALUES ($RANDOM);
            COMMIT;" 2>&1| grep -Fa "Exception: " | grep -Fv UNKNOWN_STATUS_OF_TRANSACTION
  done
}

function introspection()
{
  while true; do
    $CLICKHOUSE_CLIENT -q "SELECT * FROM system.transactions FORMAT Null"
    $CLICKHOUSE_CLIENT -q "SELECT transactionLatestSnapshot(), transactionOldestSnapshot() FORMAT Null"
  done
}

export -f begin_commit_readonly
export -f begin_rollback_readonly
export -f begin_insert_commit
export -f introspection

TIMEOUT=20

timeout $TIMEOUT bash -c begin_commit_readonly &
timeout $TIMEOUT bash -c begin_rollback_readonly &
timeout $TIMEOUT bash -c begin_insert_commit &
timeout $TIMEOUT bash -c introspection &

wait

$CLICKHOUSE_CLIENT --query "DROP TABLE mt";
