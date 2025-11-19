#!/usr/bin/env bash
# Tags: no-random-settings, no-random-merge-tree-settings
CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS test_with_ttl"

$CLICKHOUSE_CLIENT --query "CREATE TABLE test_with_ttl (d DateTime, a Int) engine = MergeTree order by tuple() TTL d + INTERVAL 1 SECOND settings min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage=0, vertical_merge_algorithm_min_columns_to_activate=1, vertical_merge_algorithm_min_rows_to_activate=1"

$CLICKHOUSE_CLIENT --query "SYSTEM STOP MERGES test_with_ttl"

$CLICKHOUSE_CLIENT --query "INSERT INTO test_with_ttl VALUES (now(), 1)"
$CLICKHOUSE_CLIENT --query "INSERT INTO test_with_ttl VALUES (now(), 2)"
$CLICKHOUSE_CLIENT --query "INSERT INTO test_with_ttl VALUES (now(), 3)"
$CLICKHOUSE_CLIENT --query "INSERT INTO test_with_ttl VALUES (now(), 4)"
$CLICKHOUSE_CLIENT --query "INSERT INTO test_with_ttl VALUES (now(), 5)"
$CLICKHOUSE_CLIENT --query "INSERT INTO test_with_ttl VALUES (now(), 6)"
$CLICKHOUSE_CLIENT --query "INSERT INTO test_with_ttl VALUES (now(), 7)"

$CLICKHOUSE_CLIENT --query "ALTER TABLE test_with_ttl REMOVE TTL"

# to trigger TTL
# NOTE: it doesn't make test flaky, if TTL is not ready than merge will be Vertical by default
sleep 3

$CLICKHOUSE_CLIENT --query "SYSTEM START MERGES test_with_ttl"

iterations=30
for _ in $(seq 1 $iterations); do
    $CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS part_log"
    merge_type=$($CLICKHOUSE_CLIENT --query "SELECT merge_algorithm FROM system.part_log WHERE table = 'test_with_ttl' and database='${CLICKHOUSE_DATABASE}' and event_type = 'MergeParts' LIMIT 1")
    if [[ "$merge_type" != "" && "$merge_type" != "Undecided" ]]; then
        break;
    fi
    sleep 1
done

echo "Got merge algorithm $merge_type"

$CLICKHOUSE_CLIENT --query "DETACH TABLE test_with_ttl"
$CLICKHOUSE_CLIENT --query "ATTACH TABLE test_with_ttl"

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS test_with_ttl"
