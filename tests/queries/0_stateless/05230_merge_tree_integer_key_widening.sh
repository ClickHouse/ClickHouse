#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -euo pipefail

client()
{
    $CLICKHOUSE_CLIENT --query "$1"
}

expect_failure()
{
    local query=$1
    local description=$2

    if $CLICKHOUSE_CLIENT --query "$query" >/dev/null 2>&1; then
        echo "unexpected success: $description"
        exit 1
    fi

    echo "rejected: $description"
}

wait_for_mutation()
{
    local table=$1

    for _ in $(seq 1 300); do
        if [ "$($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = '$table' AND NOT is_done")" -eq 0 ]; then
            return
        fi
        sleep 0.1
    done

    echo "mutation did not finish: $table"
    exit 1
}

cleanup()
{
    for table in merge_tree_integer_key_widening merge_tree_sorting_key_widening merge_tree_signed_key_widening merge_tree_enum_key_widening merge_tree_integer_key_widening_reject merge_tree_partition_key_widening merge_tree_sample_key_widening; do
        $CLICKHOUSE_CLIENT --query "SYSTEM START MERGES $table" >/dev/null 2>&1 || true
        $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS $table" >/dev/null 2>&1 || true
    done
}
trap cleanup EXIT

client "DROP TABLE IF EXISTS merge_tree_integer_key_widening"
client "CREATE TABLE merge_tree_integer_key_widening (k UInt16, value String) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0"
client "SYSTEM STOP MERGES merge_tree_integer_key_widening"
client "INSERT INTO merge_tree_integer_key_widening VALUES (1, 'one'), (65535, 'old-max')"
client "ALTER TABLE merge_tree_integer_key_widening MODIFY COLUMN k UInt32 SETTINGS mutations_sync = 0, alter_sync = 0"
client "DETACH TABLE merge_tree_integer_key_widening"
client "ATTACH TABLE merge_tree_integer_key_widening"
client "SELECT toTypeName(k), k, value FROM merge_tree_integer_key_widening WHERE k IN (1, 65535) ORDER BY k"
client "SELECT toTypeName(k), k, value FROM merge_tree_integer_key_widening WHERE k IN (1, 65535) ORDER BY k"
client "INSERT INTO merge_tree_integer_key_widening VALUES (70000, 'new')"
client "SELECT toTypeName(k), k, value FROM merge_tree_integer_key_widening ORDER BY k"
client "SYSTEM START MERGES merge_tree_integer_key_widening"
wait_for_mutation merge_tree_integer_key_widening
client "SELECT count(), min(k), max(k), toTypeName(k) FROM merge_tree_integer_key_widening"

client "DROP TABLE IF EXISTS merge_tree_sorting_key_widening"
client "CREATE TABLE merge_tree_sorting_key_widening (tenant UInt8, k UInt16, value String) ENGINE = MergeTree ORDER BY (tenant, k) PRIMARY KEY tenant SETTINGS index_granularity = 1, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0"
client "SYSTEM STOP MERGES merge_tree_sorting_key_widening"
client "INSERT INTO merge_tree_sorting_key_widening VALUES (1, 1, 'one'), (1, 65535, 'old-max')"
client "ALTER TABLE merge_tree_sorting_key_widening MODIFY COLUMN k UInt32 SETTINGS mutations_sync = 0, alter_sync = 0"
client "SELECT toTypeName(k), tenant, k, value FROM merge_tree_sorting_key_widening ORDER BY tenant, k"
client "SYSTEM START MERGES merge_tree_sorting_key_widening"
wait_for_mutation merge_tree_sorting_key_widening

client "DROP TABLE IF EXISTS merge_tree_signed_key_widening"
client "CREATE TABLE merge_tree_signed_key_widening (k Int8, value String) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0"
client "SYSTEM STOP MERGES merge_tree_signed_key_widening"
client "INSERT INTO merge_tree_signed_key_widening VALUES (-128, 'old-min'), (127, 'old-max')"
client "ALTER TABLE merge_tree_signed_key_widening MODIFY COLUMN k Int64 SETTINGS mutations_sync = 0, alter_sync = 0"
client "SELECT toTypeName(k), k, value FROM merge_tree_signed_key_widening ORDER BY k"
client "SYSTEM START MERGES merge_tree_signed_key_widening"
wait_for_mutation merge_tree_signed_key_widening

client "DROP TABLE IF EXISTS merge_tree_enum_key_widening"
client "CREATE TABLE merge_tree_enum_key_widening (k Enum8('one' = 1, 'two' = 2), value String) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0"
client "SYSTEM STOP MERGES merge_tree_enum_key_widening"
client "INSERT INTO merge_tree_enum_key_widening VALUES (1, 'a'), (2, 'b')"
client "ALTER TABLE merge_tree_enum_key_widening MODIFY COLUMN k Int8 SETTINGS mutations_sync = 0, alter_sync = 0"
client "ALTER TABLE merge_tree_enum_key_widening MODIFY COLUMN k Int16 SETTINGS mutations_sync = 0, alter_sync = 0"
client "SYSTEM START MERGES merge_tree_enum_key_widening"
wait_for_mutation merge_tree_enum_key_widening
client "DETACH TABLE merge_tree_enum_key_widening"
client "ATTACH TABLE merge_tree_enum_key_widening"
client "SELECT toTypeName(k), k, value FROM merge_tree_enum_key_widening WHERE k IN (1, 2) ORDER BY k"

client "DROP TABLE IF EXISTS merge_tree_integer_key_widening_reject"
client "CREATE TABLE merge_tree_integer_key_widening_reject (k UInt32) ENGINE = MergeTree ORDER BY k"
expect_failure "ALTER TABLE merge_tree_integer_key_widening_reject MODIFY COLUMN k UInt16" "shrinking integer key"
expect_failure "ALTER TABLE merge_tree_integer_key_widening_reject MODIFY COLUMN k Int64" "signedness change"
expect_failure "ALTER TABLE merge_tree_integer_key_widening_reject MODIFY COLUMN k Float64" "integer to float"
expect_failure "ALTER TABLE merge_tree_integer_key_widening_reject MODIFY COLUMN k UInt128" "native integer to UInt128"

client "DROP TABLE IF EXISTS merge_tree_partition_key_widening"
client "CREATE TABLE merge_tree_partition_key_widening (k UInt16) ENGINE = MergeTree PARTITION BY k ORDER BY k"
expect_failure "ALTER TABLE merge_tree_partition_key_widening MODIFY COLUMN k UInt32" "partition key widening"

client "DROP TABLE IF EXISTS merge_tree_sample_key_widening"
client "CREATE TABLE merge_tree_sample_key_widening (k UInt16) ENGINE = MergeTree ORDER BY k SAMPLE BY k"
expect_failure "ALTER TABLE merge_tree_sample_key_widening MODIFY COLUMN k UInt32" "sampling key widening"
