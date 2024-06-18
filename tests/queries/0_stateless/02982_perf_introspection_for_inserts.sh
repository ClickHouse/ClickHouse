#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


$CLICKHOUSE_CLIENT -q """
CREATE TABLE t02982
(
    n UInt64,
    s Nullable(String),
    INDEX idx1 n TYPE minmax GRANULARITY 2,
    INDEX idx2 n * length(s) TYPE set(1000) GRANULARITY 2,
    PROJECTION pr_sort
    (
        SELECT
            n,
            sum(length(s))
        GROUP BY n
    )
)
ENGINE = MergeTree
ORDER BY n;
"""

query_id=$RANDOM

$CLICKHOUSE_CLIENT --query_id $query_id -q """
INSERT INTO t02982 SELECT
    number,
    'a'
FROM numbers_mt(1000000);
"""

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS"
$CLICKHOUSE_CLIENT -q """
SELECT
    ProfileEvents['MergeTreeDataProjectionWriterMergingBlocksMicroseconds'] = 0,
    ProfileEvents['MergeTreeDataProjectionWriterSortingBlocksMicroseconds'] > 0,
    ProfileEvents['MergeTreeDataWriterSortingBlocksMicroseconds'] > 0,
    ProfileEvents['MergeTreeDataWriterProjectionsCalculationMicroseconds'] > 0,
    ProfileEvents['MergeTreeDataWriterSkipIndicesCalculationMicroseconds'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND query_id='$query_id' AND type = 'QueryFinish';
"""
