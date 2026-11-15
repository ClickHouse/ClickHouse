#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -n --query "
CREATE TABLE test (d Date, x UInt64) ENGINE = MergeTree() PARTITION BY d ORDER BY x SETTINGS index_granularity = 5;
INSERT INTO test VALUES ('2025-01-01', 1), ('2025-01-02', 2);
"

for enable_analyzer in 0 1; do
# should filter by virtual columns
$CLICKHOUSE_CLIENT --query "SELECT * FROM test WHERE _partition_id = '202501' FORMAT Null" --enable_analyzer=$enable_analyzer --print-profile-events 2>&1 \
    | grep -q FilterPartsByVirtualColumnsMicroseconds && echo "enable_analyzer: $enable_analyzer, filtering by virtual columns"
# should not filter by virtual columns
$CLICKHOUSE_CLIENT --query "SELECT * FROM test WHERE d = '2025-01-01' FORMAT Null" --enable_analyzer=$enable_analyzer --print-profile-events 2>&1 \
    | grep -q FilterPartsByVirtualColumnsMicroseconds && echo "enable_analyzer: $enable_analyzer, should not filter by virtual columns"
$CLICKHOUSE_CLIENT --query "SELECT * FROM test WHERE d = '2025-01-01' AND 1 FORMAT Null" --enable_analyzer=$enable_analyzer --print-profile-events 2>&1 \
    | grep -q FilterPartsByVirtualColumnsMicroseconds && echo "enable_analyzer: $enable_analyzer, should not filter by virtual columns"
done

$CLICKHOUSE_CLIENT -n --query "
DROP TABLE test;
"
