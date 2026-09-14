#!/usr/bin/env bash
# Tags: no-replicated-database, no-ordinary-database, no-shared-merge-tree, no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./transactions.lib
. "$CUR_DIR"/transactions.lib

# A still-open transaction's part below the mutation version keeps the entry alive after every
# visible part is rewritten (its committing-block holder is released when the INSERT statement
# finishes, so only the part marks the pending work): is_done must stay 0 with parts_to_do = 0 and
# progress NULL, and the entry finishes only once the transaction commits and its part is rewritten.

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_mut_uncommitted SYNC"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_mut_uncommitted (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_mut_uncommitted VALUES (1, 1)"

tx 1 "BEGIN TRANSACTION" > /dev/null
tx 1 "INSERT INTO t_mut_uncommitted SETTINGS async_insert = 0 VALUES (2, 2)" > /dev/null

$CLICKHOUSE_CLIENT -q "ALTER TABLE t_mut_uncommitted UPDATE v = v + 1 WHERE 1 SETTINGS mutations_sync = 0"

state=""
for _ in {1..300}; do
    state=$($CLICKHOUSE_CLIENT -q "SELECT is_done, parts_to_do, bytes_to_do, progress FROM system.mutations WHERE database = currentDatabase() AND table = 't_mut_uncommitted' AND NOT is_done AND parts_to_do = 0")
    [ -n "$state" ] && break
    sleep 0.2
done
echo "${state:-FAIL: never reached parts_to_do = 0 while not done}"

tx 1 "COMMIT" > /dev/null

done_state=""
for _ in {1..300}; do
    done_state=$($CLICKHOUSE_CLIENT -q "SELECT is_done, progress FROM system.mutations WHERE database = currentDatabase() AND table = 't_mut_uncommitted' AND is_done")
    [ -n "$done_state" ] && break
    sleep 0.2
done
echo "${done_state:-FAIL: mutation never finished}"

$CLICKHOUSE_CLIENT -q "SELECT k, v FROM t_mut_uncommitted ORDER BY k"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_mut_uncommitted SYNC"

# The rolled-back sibling: the transactional part must never enter the byte denominator, so the
# mutation still ends at progress 1 with only the committed row rewritten.
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_mut_rolled_back SYNC"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_mut_rolled_back (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_mut_rolled_back VALUES (1, 1)"

tx 2 "BEGIN TRANSACTION" > /dev/null
tx 2 "INSERT INTO t_mut_rolled_back SETTINGS async_insert = 0 VALUES (2, 2)" > /dev/null

$CLICKHOUSE_CLIENT -q "ALTER TABLE t_mut_rolled_back UPDATE v = v + 1 WHERE 1 SETTINGS mutations_sync = 0"

tx 2 "ROLLBACK" > /dev/null

rb_state=""
for _ in {1..300}; do
    rb_state=$($CLICKHOUSE_CLIENT -q "SELECT is_done, progress FROM system.mutations WHERE database = currentDatabase() AND table = 't_mut_rolled_back' AND is_done")
    [ -n "$rb_state" ] && break
    sleep 0.2
done
echo "${rb_state:-FAIL: mutation never finished after the rollback}"

$CLICKHOUSE_CLIENT -q "SELECT k, v FROM t_mut_rolled_back ORDER BY k"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_mut_rolled_back SYNC"

# The mutation's own transaction sees its uncommitted part and will rewrite it, so that part is
# known scope, not unknown work: with merges stopped, both parts count and progress is a number.
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_mut_same_tx SYNC"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_mut_same_tx (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_mut_same_tx VALUES (1, 1)"

tx 3 "BEGIN TRANSACTION" > /dev/null
tx 3 "INSERT INTO t_mut_same_tx SETTINGS async_insert = 0 VALUES (2, 2)" > /dev/null

$CLICKHOUSE_CLIENT -q "SYSTEM STOP MERGES t_mut_same_tx"
# A transactional mutation always waits for completion, so it runs asynchronously while the
# merges-stopped state is observed.
tx_async 3 "ALTER TABLE t_mut_same_tx UPDATE v = v + 1 WHERE 1" > /dev/null

same_tx_state=""
for _ in {1..300}; do
    same_tx_state=$($CLICKHOUSE_CLIENT -q "SELECT is_done, parts_to_do, bytes_to_do > 0, progress FROM system.mutations WHERE database = currentDatabase() AND table = 't_mut_same_tx'")
    [ -n "$same_tx_state" ] && break
    sleep 0.2
done
echo "${same_tx_state:-FAIL: same-transaction mutation never appeared}"

$CLICKHOUSE_CLIENT -q "SYSTEM START MERGES t_mut_same_tx"
tx_wait 3 > /dev/null
tx 3 "COMMIT" > /dev/null

same_tx_done=""
for _ in {1..300}; do
    same_tx_done=$($CLICKHOUSE_CLIENT -q "SELECT is_done, progress FROM system.mutations WHERE database = currentDatabase() AND table = 't_mut_same_tx' AND is_done")
    [ -n "$same_tx_done" ] && break
    sleep 0.2
done
echo "${same_tx_done:-FAIL: same-transaction mutation never finished}"

$CLICKHOUSE_CLIENT -q "SELECT k, v FROM t_mut_same_tx ORDER BY k"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_mut_same_tx SYNC"

# A transactional mutation is scoped by its own snapshot, so another transaction's uncommitted part
# is invisible to it and never becomes its work: the entry must reach done while that other
# transaction is still open, instead of hanging on a block number it will never rewrite.
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_mut_other_tx SYNC"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_mut_other_tx (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_mut_other_tx VALUES (1, 1)"

tx 4 "BEGIN TRANSACTION" > /dev/null
tx 4 "INSERT INTO t_mut_other_tx SETTINGS async_insert = 0 VALUES (2, 2)" > /dev/null

tx 5 "BEGIN TRANSACTION" > /dev/null
tx 5 "ALTER TABLE t_mut_other_tx UPDATE v = v + 1 WHERE 1" > /dev/null
tx 5 "COMMIT" > /dev/null

other_tx_done=""
for _ in {1..300}; do
    other_tx_done=$($CLICKHOUSE_CLIENT -q "SELECT is_done, progress FROM system.mutations WHERE database = currentDatabase() AND table = 't_mut_other_tx' AND is_done")
    [ -n "$other_tx_done" ] && break
    sleep 0.2
done
echo "${other_tx_done:-FAIL: mutation blocked by an invisible other-transaction part}"

tx 4 "ROLLBACK" > /dev/null
$CLICKHOUSE_CLIENT -q "SELECT k, v FROM t_mut_other_tx ORDER BY k"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_mut_other_tx SYNC"
