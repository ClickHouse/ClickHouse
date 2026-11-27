#!/usr/bin/env bash
# Tags: long, no-ordinary-database, no-encrypted-storage

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS mt";
$CLICKHOUSE_CLIENT --query "CREATE TABLE mt (n Int64) ENGINE=MergeTree ORDER BY n SETTINGS old_parts_lifetime=0";


function begin_commit_readonly()
{
  local TIMELIMIT=$((SECONDS+TIMEOUT))
  while [ $SECONDS -lt "$TIMELIMIT" ]
  do
    $CLICKHOUSE_CLIENT --query "
            SET wait_changes_become_visible_after_commit_mode='wait';
            BEGIN TRANSACTION;
            COMMIT;" 2>&1| grep -Fa "Exception: " | grep -Fv UNKNOWN_STATUS_OF_TRANSACTION
  done
}

function begin_rollback_readonly()
{
  local TIMELIMIT=$((SECONDS+TIMEOUT))
  while [ $SECONDS -lt "$TIMELIMIT" ]
  do
    $CLICKHOUSE_CLIENT --wait_changes_become_visible_after_commit_mode=wait_unknown --query "
            BEGIN TRANSACTION;
            SET TRANSACTION SNAPSHOT 42;
            ROLLBACK;"
  done
}

function begin_insert_commit()
{
  local TIMELIMIT=$((SECONDS+TIMEOUT))
  while [ $SECONDS -lt "$TIMELIMIT" ]
  do
    $CLICKHOUSE_CLIENT --wait_changes_become_visible_after_commit_mode=async --query "
            BEGIN TRANSACTION;
            INSERT INTO mt VALUES ($RANDOM);
            COMMIT;" 2>&1| grep -Fa "Exception: " | grep -Fv UNKNOWN_STATUS_OF_TRANSACTION
  done
}

function introspection()
{
  local TIMELIMIT=$((SECONDS+TIMEOUT))
  while [ $SECONDS -lt "$TIMELIMIT" ]
  do
    $CLICKHOUSE_CLIENT -q "SELECT * FROM system.transactions FORMAT Null"
    $CLICKHOUSE_CLIENT -q "SELECT transactionLatestSnapshot(), transactionOldestSnapshot() FORMAT Null"
  done
}

TIMEOUT=20

begin_commit_readonly &
begin_rollback_readonly &
begin_insert_commit &
introspection &

wait

$CLICKHOUSE_CLIENT --query "DROP TABLE mt";
