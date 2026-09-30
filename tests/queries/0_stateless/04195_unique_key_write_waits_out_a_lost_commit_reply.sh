#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-ordinary-database, no-replicated-database, no-shared-merge-tree
# UNIQUE KEY: an INSERT and a DELETE whose commit reply is lost wait until the transaction resolves,
# then succeed with their rows visible. Red if either reports UNKNOWN_STATUS_OF_TRANSACTION.
# no-parallel: `transaction_force_unknown_state_after_commit` is server-wide.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

CLICKHOUSE_CLIENT="${CLICKHOUSE_CLIENT} --enable_unique_key 1"

cleanup() {
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT transaction_force_unknown_state_after_commit" 2>/dev/null || true
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS uk_lost_reply"
$CLICKHOUSE_CLIENT --query "
    CREATE TABLE uk_lost_reply (id UInt64, v String)
    ENGINE = MergeTree ORDER BY id UNIQUE KEY (id)"
$CLICKHOUSE_CLIENT --query "INSERT INTO uk_lost_reply SELECT number, 'a' FROM numbers(5)"

# The commit lands in Keeper and its reply is dropped, so the status is known only once the
# transaction log's updating thread finds the csn entry.
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT transaction_force_unknown_state_after_commit"
$CLICKHOUSE_CLIENT --query "INSERT INTO uk_lost_reply SELECT number, 'b' FROM numbers(3, 4)"
$CLICKHOUSE_CLIENT --query "DELETE FROM uk_lost_reply WHERE id = 1"
$CLICKHOUSE_CLIENT --query "SELECT id, v FROM uk_lost_reply ORDER BY id"
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT transaction_force_unknown_state_after_commit"

$CLICKHOUSE_CLIENT --query "DROP TABLE uk_lost_reply"
