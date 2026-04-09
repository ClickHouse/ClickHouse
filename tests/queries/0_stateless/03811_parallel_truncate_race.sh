#!/usr/bin/env bash
# Tags: race, no-parallel, long, no-replicated-database, no-parallel-replicas, no-object-storage
# no-replicated-database: Distributed DDL queries (TRUNCATE TABLE) inside transactions are not supported

# Stress test for concurrent TRUNCATE operations with transactions and PARALLEL WITH.
#
# This tests the fix for a bug where stale tmp_empty_* directories could cause
# "directory already exists" errors. The original bug occurred when:
# 1. TRUNCATE creates tmp_empty_* directories for new empty parts
# 2. TRUNCATE fails (e.g., PART_IS_TEMPORARILY_LOCKED during rename)
# 3. Transaction rollback removes parts from memory but leaves tmp_empty_* dirs on disk
# 4. Next TRUNCATE tries to create the same tmp_empty_* directories and fails
#
# The fix handles this by removing existing tmp_empty_* directories before creating new ones.
#
# This test runs concurrent truncates with:
# - PARALLEL WITH TRUNCATE (both truncates on same table)
# - TRUNCATE inside transactions with COMMIT
# - TRUNCATE inside transactions with ROLLBACK
# - Concurrent INSERTs to create more parts
#
# The exact race is hard to trigger deterministically, but this stress test exercises
# the code paths involved.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_parallel_truncate"
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_parallel_truncate (key UInt64, value String)
    ENGINE = MergeTree
    PARTITION BY key % 10
    ORDER BY key
"

function parallel_truncate_thread() {
    local TIMELIMIT=$((SECONDS+$1))
    while [ $SECONDS -lt "$TIMELIMIT" ]; do
        $CLICKHOUSE_CLIENT -q "TRUNCATE TABLE t_parallel_truncate PARALLEL WITH TRUNCATE TABLE t_parallel_truncate" 2>&1 \
            | grep -Fa "Exception: " \
            | grep -Fv "TABLE_IS_DROPPED" \
            | grep -Fv "UNKNOWN_TABLE" \
            | grep -Fv "PART_IS_TEMPORARILY_LOCKED" \
            | grep -Fv "SERIALIZATION_ERROR" \
            || true
    done
}

function txn_truncate_thread() {
    local TIMELIMIT=$((SECONDS+$1))
    while [ $SECONDS -lt "$TIMELIMIT" ]; do
        $CLICKHOUSE_CLIENT --multiquery -q "
        BEGIN TRANSACTION;
        TRUNCATE TABLE t_parallel_truncate;
        COMMIT;
        " 2>&1 \
            | grep -Fa "Exception: " \
            | grep -Fv "TABLE_IS_DROPPED" \
            | grep -Fv "UNKNOWN_TABLE" \
            | grep -Fv "PART_IS_TEMPORARILY_LOCKED" \
            | grep -Fv "SERIALIZATION_ERROR" \
            || true
    done
}

function txn_truncate_rollback_thread() {
    local TIMELIMIT=$((SECONDS+$1))
    while [ $SECONDS -lt "$TIMELIMIT" ]; do
        $CLICKHOUSE_CLIENT --multiquery -q "
        BEGIN TRANSACTION;
        TRUNCATE TABLE t_parallel_truncate;
        ROLLBACK;
        " 2>&1 \
            | grep -Fa "Exception: " \
            | grep -Fv "TABLE_IS_DROPPED" \
            | grep -Fv "UNKNOWN_TABLE" \
            | grep -Fv "PART_IS_TEMPORARILY_LOCKED" \
            | grep -Fv "SERIALIZATION_ERROR" \
            || true
    done
}

function insert_thread() {
    local TIMELIMIT=$((SECONDS+$1))
    while [ $SECONDS -lt "$TIMELIMIT" ]; do
        $CLICKHOUSE_CLIENT -q "INSERT INTO t_parallel_truncate SELECT number, 'value' FROM numbers(100)" 2>&1 \
            | grep -Fa "Exception: " \
            | grep -Fv "TABLE_IS_DROPPED" \
            | grep -Fv "UNKNOWN_TABLE" \
            || true
    done
}

function optimize_thread() {
    local TIMELIMIT=$((SECONDS+$1))
    while [ $SECONDS -lt "$TIMELIMIT" ]; do
        $CLICKHOUSE_CLIENT -q "OPTIMIZE TABLE t_parallel_truncate FINAL" 2>&1 \
            | grep -Fa "Exception: " \
            | grep -Fv "TABLE_IS_DROPPED" \
            | grep -Fv "UNKNOWN_TABLE" \
            | grep -Fv "PART_IS_TEMPORARILY_LOCKED" \
            | grep -Fv "SERIALIZATION_ERROR" \
            | grep -Fv "CANNOT_ASSIGN_OPTIMIZE" \
            | grep -Fv "ABORTED" \
            || true
    done
}

# Initial data
$CLICKHOUSE_CLIENT -q "INSERT INTO t_parallel_truncate SELECT number, 'value' FROM numbers(5000)"

TIMEOUT=10

# Run concurrent operations
parallel_truncate_thread $TIMEOUT &
parallel_truncate_thread $TIMEOUT &
parallel_truncate_thread $TIMEOUT &
parallel_truncate_thread $TIMEOUT &
parallel_truncate_thread $TIMEOUT &
parallel_truncate_thread $TIMEOUT &
parallel_truncate_thread $TIMEOUT &
parallel_truncate_thread $TIMEOUT &
txn_truncate_thread $TIMEOUT &
txn_truncate_thread $TIMEOUT &
txn_truncate_rollback_thread $TIMEOUT &
txn_truncate_rollback_thread $TIMEOUT &
txn_truncate_rollback_thread $TIMEOUT &
txn_truncate_rollback_thread $TIMEOUT &
insert_thread $TIMEOUT &

wait

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_parallel_truncate"

echo "OK"
