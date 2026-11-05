#!/usr/bin/env bash
# Tags: no-ordinary-database, no-replicated-database
#
# `REPLACE PARTITION FROM` and `MOVE PARTITION TO TABLE` commit their new parts *before* removing
# the old ones. Refusing the removal at that point (05161) would leave the destination half
# replaced or half moved, so both statements have to notice an in-flight transactional creator
# before they publish anything of their own.
#
# `MOVE` uses the stricter check of the two: it republishes the rows in another table, where a
# commit cannot be taken back, so the creating transaction must be committed -- not merely
# "no longer running".

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./transactions.lib
. "$CUR_DIR"/transactions.lib

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_replace_dst"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_replace_src"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_replace_dst (x UInt64) ENGINE = MergeTree ORDER BY x"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_replace_src (x UInt64) ENGINE = MergeTree ORDER BY x"

$CLICKHOUSE_CLIENT -q "INSERT INTO t_replace_src SETTINGS async_insert = 0 VALUES (100)"

# The destination partition holds a part whose creating transaction is still running.
tx 1 "BEGIN TRANSACTION"
tx 1 "INSERT INTO t_replace_dst SETTINGS async_insert = 0 VALUES (1)"

$CLICKHOUSE_CLIENT -q "ALTER TABLE t_replace_dst REPLACE PARTITION tuple() FROM t_replace_src" 2>&1 \
    | grep -o -F "SERIALIZATION_ERROR" | head -1

tx 1 "COMMIT"

# The refused REPLACE published nothing: the destination still holds only its own row.
$CLICKHOUSE_CLIENT -q "SELECT x FROM t_replace_dst ORDER BY x"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM t_replace_src"

# Retrying after the transaction finished works, and replaces everything.
$CLICKHOUSE_CLIENT -q "ALTER TABLE t_replace_dst REPLACE PARTITION tuple() FROM t_replace_src"
$CLICKHOUSE_CLIENT -q "SELECT x FROM t_replace_dst ORDER BY x"

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_move_dst"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_move_src"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_move_dst (x UInt64) ENGINE = MergeTree ORDER BY x"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_move_src (x UInt64) ENGINE = MergeTree ORDER BY x"

$CLICKHOUSE_CLIENT -q "INSERT INTO t_move_src SETTINGS async_insert = 0 VALUES (200)"

# The source partition holds a part whose creating transaction is still running.
tx 2 "BEGIN TRANSACTION"
tx 2 "INSERT INTO t_move_src SETTINGS async_insert = 0 VALUES (2)"

$CLICKHOUSE_CLIENT -q "ALTER TABLE t_move_src MOVE PARTITION tuple() TO TABLE t_move_dst" 2>&1 \
    | grep -o -F "SERIALIZATION_ERROR" | head -1

tx 2 "COMMIT"

# The refused MOVE published nothing in the destination and removed nothing from the source.
$CLICKHOUSE_CLIENT -q "SELECT count() FROM t_move_dst"
$CLICKHOUSE_CLIENT -q "SELECT x FROM t_move_src ORDER BY x"

$CLICKHOUSE_CLIENT -q "ALTER TABLE t_move_src MOVE PARTITION tuple() TO TABLE t_move_dst"
$CLICKHOUSE_CLIENT -q "SELECT x FROM t_move_dst ORDER BY x"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM t_move_src"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_replace_dst"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_replace_src"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_move_dst"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_move_src"
