#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS set_idx;"

$CLICKHOUSE_CLIENT -n --query="
SET allow_experimental_data_skipping_indices = 1;
CREATE TABLE set_idx
(
    u64 UInt64,
    i32 Int32,
    INDEX idx (i32) TYPE set(2) GRANULARITY 1
) ENGINE = MergeTree()
ORDER BY u64
SETTINGS index_granularity = 6;"

$CLICKHOUSE_CLIENT --query="
INSERT INTO set_idx
SELECT number, number FROM system.numbers LIMIT 100"

# simple select
$CLICKHOUSE_CLIENT --query="SELECT * FROM set_idx WHERE i32 > 0 FORMAT JSON" | grep "rows_read"


$CLICKHOUSE_CLIENT --query="DROP TABLE set_idx;"