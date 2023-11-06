#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "
CREATE TABLE test (
    test_id UInt64,
    test_name String,
    test_count Nullable(Float64),
    test_string String,
    PROJECTION projection_test_by_string (
        SELECT test_string,
            sum(test_count)
        GROUP BY test_id,
            test_string,
            test_name
    ),
    PROJECTION projection_test_by_more (
        SELECT test_string,
            test_name,
            sum(test_count)
        GROUP BY test_id,
            test_string,
            test_name
    )
) ENGINE = MergeTree
ORDER BY test_string;"

$CLICKHOUSE_CLIENT -q "
INSERT INTO test
SELECT number,
    'test',
    1.* (number / 2),
    'test'
FROM numbers(100, 500);"

$CLICKHOUSE_CLIENT --query_id '02907_test' -q "
SELECT test_string
FROM test
WHERE (test_id > 50)
    AND (test_id < 150)
GROUP BY test_string;"

$CLICKHOUSE_CLIENT -q "
SELECT projections
FROM system.query_log
WHERE query_id = '02907_test' AND arrayElement(projections, 1) LIKE '%projection_test_by_string'
LIMIT 1;" | grep -o "projection_test_by_string" || true

$CLICKHOUSE_CLIENT -q "
SELECT projections
FROM system.query_log 
WHERE query_id = '02907_test' AND arrayElement(projections, 1) LIKE '%projection_test_by_more' 
LIMIT 1;" | grep -o "projection_test_by_more" || true

echo "Executing query with setting"

$CLICKHOUSE_CLIENT --query_id '02907_test_1' --preferred_optimize_projection_name 'projection_test_by_more' -q "
SELECT test_string
FROM test
WHERE (test_id > 50)
    AND (test_id < 150)
GROUP BY test_string;"

$CLICKHOUSE_CLIENT -q "
SELECT projections
FROM system.query_log
WHERE query_id = '02907_test_1' AND arrayElement(projections, 1) LIKE '%projection_test_by_more'
LIMIT 1;" | grep -o "projection_test_by_more" || true

$CLICKHOUSE_CLIENT -q "
SELECT projections
FROM system.query_log
WHERE query_id = '02907_test_1' AND arrayElement(projections, 1) LIKE '%projection_test_by_string'
LIMIT 1" | grep -o "projection_test_by_string" || true

$CLICKHOUSE_CLIENT --query_id '02907_test_2' --preferred_optimize_projection_name 'non_existing_projection' -q "
SELECT test_string
FROM test
WHERE (test_id > 50)
    AND (test_id < 150)
GROUP BY test_string;"

$CLICKHOUSE_CLIENT -q "
SELECT projections
FROM system.query_log
WHERE query_id = '02907_test_2' AND arrayElement(projections, 1) LIKE '%projection_test_by_string'
LIMIT 1"  | grep -o "projection_test_by_string" || true
