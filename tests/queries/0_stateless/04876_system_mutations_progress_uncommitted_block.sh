#!/usr/bin/env bash
# Tags: no-replicated-database, no-ordinary-database, no-shared-merge-tree, no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./transactions.lib
. "$CUR_DIR"/transactions.lib

# An uncommitted insert block below the mutation version keeps the entry alive after every visible
# part is rewritten: is_done must stay 0 with parts_to_do = 0 and the byte-weighted progress at 1,
# and the entry finishes only once the transaction commits and its part is rewritten too.

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
