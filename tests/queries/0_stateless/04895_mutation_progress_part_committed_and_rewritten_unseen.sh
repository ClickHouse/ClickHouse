#!/usr/bin/env bash
# Tags: no-replicated-database, no-ordinary-database, no-shared-merge-tree, no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./transactions.lib
. "$CUR_DIR"/transactions.lib

# A transaction's lower-block part commits after the mutation is created and is rewritten before any
# system.mutations read sees it pending: its byte weight must still count as done in `progress`.

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_mut_unseen SYNC"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_mut_unseen (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_mut_unseen VALUES (1, 1)"

tx 1 "BEGIN TRANSACTION" > /dev/null
tx 1 "INSERT INTO t_mut_unseen SETTINGS async_insert = 0 SELECT number + 2, number FROM numbers(100000)" > /dev/null

# The small committed part fails the mutation forever, so it stays pending.
$CLICKHOUSE_CLIENT -q "ALTER TABLE t_mut_unseen UPDATE v = v + throwIf(k = 1) WHERE 1 SETTINGS mutations_sync = 0"

tx 1 "COMMIT" > /dev/null

# Wait on system.parts only, so no status read sees the big part pending.
for _ in {1..600}; do
    rewritten=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_mut_unseen' AND active AND rows = 100000 AND data_version > min_block_number")
    [ "$rewritten" = "1" ] && break
    sleep 0.2
done
echo "rewritten: $rewritten"

$CLICKHOUSE_CLIENT -q "SELECT is_done, parts_to_do, progress > 0.5 FROM system.mutations WHERE database = currentDatabase() AND table = 't_mut_unseen'"

$CLICKHOUSE_CLIENT -q "KILL MUTATION WHERE database = currentDatabase() AND table = 't_mut_unseen'" > /dev/null
$CLICKHOUSE_CLIENT -q "DROP TABLE t_mut_unseen SYNC"
