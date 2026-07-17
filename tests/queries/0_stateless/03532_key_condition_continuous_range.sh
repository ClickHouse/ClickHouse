#!/usr/bin/env bash
# Tags: no-parallel-replicas
# no-parallel-replicas: the ProfileEvents with the expected values are reported on the replicas the query runs in,
# and the coordinator does not collect all ProfileEvents values.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

readonly query_prefix=$CLICKHOUSE_DATABASE

$CLICKHOUSE_CLIENT -n -q "
DROP TABLE IF EXISTS test;
CREATE TABLE test
(
    i Int16,
    timestamp DateTime64(3, 'Asia/Shanghai')
)
ENGINE = MergeTree
ORDER BY (i, timestamp);
INSERT INTO test VALUES (1, '2025-06-05 01:00:00');"

$CLICKHOUSE_CLIENT -n -q "SELECT * FROM test WHERE i = 1 and toDate(timestamp) = '2025-06-05' FORMAT Null;" --query-id="${query_prefix}_binary1"
$CLICKHOUSE_CLIENT -n -q "SELECT * FROM test WHERE i in (1) and toDate(timestamp) > '2025-06-05' FORMAT Null;" --query-id="${query_prefix}_binary2"
$CLICKHOUSE_CLIENT -n -q "SELECT * FROM test WHERE toDate(i) = '2025-06-05' and timestamp = '2025-06-05 01:00:00' FORMAT Null;" --query-id="${query_prefix}_generic1"
$CLICKHOUSE_CLIENT -n -q "SELECT * FROM test WHERE toDate(i) in ('2025-06-05') and timestamp = '2025-06-05 01:00:00' FORMAT Null;" --query-id="${query_prefix}_generic2"
$CLICKHOUSE_CLIENT -n -q "SYSTEM FLUSH LOGS query_log;"

$CLICKHOUSE_CLIENT -n -q "SELECT sum(ProfileEvents['IndexBinarySearchAlgorithm']), sum(ProfileEvents['IndexGenericExclusionSearchAlgorithm']) FROM system.query_log
    WHERE type > 1 AND event_date >= yesterday() AND query_id ILIKE '${query_prefix}_binary%' AND current_database = currentDatabase()"
$CLICKHOUSE_CLIENT -n -q "SELECT sum(ProfileEvents['IndexBinarySearchAlgorithm']), sum(ProfileEvents['IndexGenericExclusionSearchAlgorithm']) FROM system.query_log
    WHERE type > 1 AND event_date >= yesterday() AND query_id ILIKE '${query_prefix}_generic%' AND current_database = currentDatabase()"
