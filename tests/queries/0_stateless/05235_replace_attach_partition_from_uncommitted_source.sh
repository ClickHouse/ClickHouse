#!/usr/bin/env bash
# Tags: zookeeper, no-ordinary-database, no-replicated-database, no-shared-merge-tree
# Tag zookeeper: one case uses a ReplicatedMergeTree destination.
# Tag no-ordinary-database: transactions need an Atomic database, as in 05161.
# Tag no-replicated-database: `CREATE TABLE ... CLONE AS` is unsupported there (see 03231).
# Tag no-shared-merge-tree: `--replace-replicated-with-shared` substitutes an engine whose
# partition commands this check does not reach.

# `REPLACE PARTITION FROM` and `ATTACH PARTITION FROM` clone the source parts and never remove
# them, so neither reaches the removal-time check that `DROP PARTITION` and `MOVE PARTITION TO
# TABLE` go through (05161, 05175). A query with no transaction of its own sees every active
# source part, including one whose creating transaction is still running, and cloning it commits
# those rows in the destination where the source's rollback cannot reach them: the destination is
# left holding rows that were never committed anywhere.
#
# `CREATE TABLE ... CLONE AS` is built on the same statement, so it is covered as a third surface.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./transactions.lib
. "$CUR_DIR"/transactions.lib

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_republish_src"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_republish_src (x UInt64) ENGINE = MergeTree ORDER BY x"
# A separate destination per case, so each one's count is an independent assertion.
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_republish_replace (x UInt64) ENGINE = MergeTree ORDER BY x"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_republish_attach (x UInt64) ENGINE = MergeTree ORDER BY x"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_republish_control (x UInt64) ENGINE = MergeTree ORDER BY x"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_republish_replicated (x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/t_republish_replicated', 'r1') ORDER BY x"

# Case 1: REPLACE PARTITION FROM a source part whose creating transaction is still running.
tx 1 "BEGIN TRANSACTION"
tx 1 "INSERT INTO t_republish_src SETTINGS async_insert = 0 VALUES (1)"
$CLICKHOUSE_CLIENT -q "ALTER TABLE t_republish_replace REPLACE PARTITION tuple() FROM t_republish_src" 2>&1 \
    | grep -o -F "SERIALIZATION_ERROR" | head -1
tx 1 "ROLLBACK"
# The rollback removes the source row; the destination must never have received a copy of it.
$CLICKHOUSE_CLIENT -q "SELECT count() FROM t_republish_replace"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM t_republish_src"

# Case 2: ATTACH PARTITION FROM is the `replace = false` arm of the same statement, and no
# pre-existing check covers it (the destination-side one runs only when replacing).
tx 2 "BEGIN TRANSACTION"
tx 2 "INSERT INTO t_republish_src SETTINGS async_insert = 0 VALUES (2)"
$CLICKHOUSE_CLIENT -q "ALTER TABLE t_republish_attach ATTACH PARTITION tuple() FROM t_republish_src" 2>&1 \
    | grep -o -F "SERIALIZATION_ERROR" | head -1
tx 2 "ROLLBACK"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM t_republish_attach"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM t_republish_src"

# Case 3: a ReplicatedMergeTree destination reads the source parts through its own code path.
# A plain MergeTree source with a replicated destination is the supported migration direction.
tx 3 "BEGIN TRANSACTION"
tx 3 "INSERT INTO t_republish_src SETTINGS async_insert = 0 VALUES (3)"
$CLICKHOUSE_CLIENT -q "ALTER TABLE t_republish_replicated ATTACH PARTITION tuple() FROM t_republish_src" 2>&1 \
    | grep -o -F "SERIALIZATION_ERROR" | head -1
tx 3 "ROLLBACK"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM t_republish_replicated"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM t_republish_src"

# Case 4: CREATE TABLE ... CLONE AS issues the same statement internally, for every partition.
tx 4 "BEGIN TRANSACTION"
tx 4 "INSERT INTO t_republish_src SETTINGS async_insert = 0 VALUES (4)"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_republish_clone CLONE AS t_republish_src" 2>&1 \
    | grep -o -F "SERIALIZATION_ERROR" | head -1
tx 4 "ROLLBACK"
# Asked through `system.parts` because a refused CREATE may leave no table to read from at all,
# while the invariant under test is only that no part of the source was published under that name.
$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_republish_clone' AND active"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_republish_clone"

# Case 5, the in-range arm: once the source transaction has committed, the very same statement
# must succeed and carry the row. Without this the check could not be told from "refuse always",
# and it is what makes the error above retryable.
tx 5 "BEGIN TRANSACTION"
tx 5 "INSERT INTO t_republish_src SETTINGS async_insert = 0 VALUES (5)"
tx 5 "COMMIT"
$CLICKHOUSE_CLIENT -q "ALTER TABLE t_republish_control ATTACH PARTITION tuple() FROM t_republish_src"
$CLICKHOUSE_CLIENT -q "SELECT x FROM t_republish_control ORDER BY x"

# Case 6: with a ReplicatedMergeTree destination, `ATTACH PARTITION ALL FROM` walks the source
# partitions one at a time and each one is committed and its log entry enqueued before the next is
# looked at, so the refusal has to come before any partition is published. Ten committed partitions
# alongside the uncommitted one, because the destination must be left empty no matter which subset
# the statement would otherwise have copied first.
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_republish_all_src (x UInt64) ENGINE = MergeTree PARTITION BY x ORDER BY x"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_republish_all_dst (x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/t_republish_all_dst', 'r1') PARTITION BY x ORDER BY x"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_republish_all_src VALUES (1),(2),(3),(4),(5),(6),(7),(8),(9),(10)"
tx 6 "BEGIN TRANSACTION"
tx 6 "INSERT INTO t_republish_all_src SETTINGS async_insert = 0 VALUES (99)"
$CLICKHOUSE_CLIENT -q "ALTER TABLE t_republish_all_dst ATTACH PARTITION ALL FROM t_republish_all_src" 2>&1 \
    | grep -o -F "SERIALIZATION_ERROR" | head -1
tx 6 "ROLLBACK"
# A non-zero count is a statement that failed after applying part of itself.
$CLICKHOUSE_CLIENT -q "SELECT count() FROM t_republish_all_dst"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_republish_src"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_republish_replace"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_republish_attach"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_republish_control"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_republish_replicated"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_republish_all_src"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_republish_all_dst"
