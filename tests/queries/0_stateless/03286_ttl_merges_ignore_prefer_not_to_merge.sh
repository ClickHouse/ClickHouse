#!/usr/bin/env bash

set -eo pipefail

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Wait for number of parts in table $1 to become $2.
# Print the changed value. If no changes for $3 seconds, prints initial value.
wait_for_number_of_parts() {
    for _ in `seq $3`
    do
        sleep 1
        res=`$CLICKHOUSE_CLIENT -q "SELECT count(*) FROM system.parts WHERE database = currentDatabase() AND table='$1' AND active"`
        if [ "$res" -eq "$2" ]
        then
            echo "$res"
            return
        fi
    done
    echo "$res"
}

ch="$CLICKHOUSE_CLIENT --stacktrace"

$ch "DROP TABLE IF EXISTS test_03286_ttl_disabled_merges"

$ch <<EOF
CREATE TABLE test_03286_ttl_disabled_merges 
  (id UInt64,
   value UInt64,
   event_time DateTime)
ENGINE MergeTree()
PARTITION BY value
ORDER BY id
SETTINGS
 storage_policy = 'test_policy_with_disabled_merges',
 min_bytes_for_wide_part = 1000000000,
 index_granularity = 8192,
 merge_with_ttl_timeout=0,
 materialize_ttl_recalculate_only=1
EOF


# Test 1: TTL merge dropping entire part ignores prefer_not_to_merge
$ch "INSERT INTO test_03286_ttl_disabled_merges SELECT number, 1, now() - INTERVAL 4 MONTH FROM numbers(1000)"
$ch "ALTER TABLE test_03286_ttl_disabled_merges MOVE PARTITION '1' TO VOLUME 'no_merge'"
$ch "SELECT count() FROM test_03286_ttl_disabled_merges"

$ch <<EOF
ALTER TABLE test_03286_ttl_disabled_merges 
MODIFY TTL event_time + INTERVAL 3 MONTH
SETTINGS mutations_sync = 1, materialize_ttl_after_modify = 1;
EOF

wait_for_number_of_parts 'test_03286_ttl_disabled_merges' 0 100

# Test 2: OPTIMIZE ignore prefer_not_to_merge
$ch "ALTER TABLE test_03286_ttl_disabled_merges REMOVE TTL SETTINGS mutations_sync = 1, materialize_ttl_after_modify = 1"

# Make part with partially expired TTL for rows
$ch "INSERT INTO test_03286_ttl_disabled_merges SELECT number, 2, now() - INTERVAL 4 MONTH FROM numbers(1000)"
$ch "INSERT INTO test_03286_ttl_disabled_merges SELECT number, 2, now() - INTERVAL 2 MONTH FROM numbers(1000)"
$ch "OPTIMIZE TABLE test_03286_ttl_disabled_merges FINAL SETTINGS optimize_throw_if_noop=true"

wait_for_number_of_parts 'test_03286_ttl_disabled_merges' 1 100

$ch "ALTER TABLE test_03286_ttl_disabled_merges MOVE PARTITION '2' TO VOLUME 'no_merge'"

$ch <<EOF
ALTER TABLE test_03286_ttl_disabled_merges 
MODIFY TTL event_time + INTERVAL 3 MONTH
SETTINGS mutations_sync = 1, materialize_ttl_after_modify = 1;
EOF

$ch "SELECT count() FROM test_03286_ttl_disabled_merges"
$ch "OPTIMIZE TABLE test_03286_ttl_disabled_merges FINAL SETTINGS optimize_throw_if_noop=true"
$ch "SELECT count() FROM test_03286_ttl_disabled_merges"
