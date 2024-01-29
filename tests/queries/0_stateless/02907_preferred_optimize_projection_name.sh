#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS test_opt_proj;"

$CLICKHOUSE_CLIENT -q "
CREATE TABLE test_opt_proj (
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
INSERT INTO test_opt_proj
SELECT number,
    'test',
    1.* (number / 2),
    'test'
FROM numbers(100, 500);"

$CLICKHOUSE_CLIENT --query_id 02907_test_$CLICKHOUSE_DATABASE -q "
SELECT test_string
FROM test_opt_proj
WHERE (test_id > 50)
    AND (test_id < 150)
GROUP BY test_string;"

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS;"

$CLICKHOUSE_CLIENT -q "
SELECT projections
FROM system.query_log
WHERE query_id = '02907_test_$CLICKHOUSE_DATABASE' AND current_database=currentDatabase()
LIMIT 1;" | grep -o "projection_test_by_string" || true

$CLICKHOUSE_CLIENT -q "
SELECT projections
FROM system.query_log 
WHERE query_id = '02907_test_$CLICKHOUSE_DATABASE' AND current_database=currentDatabase()
LIMIT 1;" | grep -o "projection_test_by_more" || true

echo "Executing query with setting"

$CLICKHOUSE_CLIENT --query_id 02907_test_1_$CLICKHOUSE_DATABASE --preferred_optimize_projection_name 'projection_test_by_more' -q "
SELECT test_string
FROM test_opt_proj
WHERE (test_id > 50)
    AND (test_id < 150)
GROUP BY test_string;"

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS;"

$CLICKHOUSE_CLIENT -q "
SELECT projections
FROM system.query_log
WHERE query_id = '02907_test_1_$CLICKHOUSE_DATABASE' AND current_database=currentDatabase()
LIMIT 1;" | grep -o "projection_test_by_more" || true

$CLICKHOUSE_CLIENT -q "
SELECT projections
FROM system.query_log
WHERE query_id = '02907_test_1_$CLICKHOUSE_DATABASE' AND current_database=currentDatabase()
LIMIT 1" | grep -o "projection_test_by_string" || true

echo "Executing query with wrong projection"

$CLICKHOUSE_CLIENT --query_id 02907_test_2_$CLICKHOUSE_DATABASE --preferred_optimize_projection_name 'non_existing_projection' -q "
SELECT test_string
FROM test_opt_proj
WHERE (test_id > 50)
    AND (test_id < 150)
GROUP BY test_string;"

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS;"

$CLICKHOUSE_CLIENT -q "
SELECT projections
FROM system.query_log
WHERE query_id = '02907_test_2_$CLICKHOUSE_DATABASE' AND current_database=currentDatabase()
LIMIT 1;" | grep -o "projection_test_by_string" || true

$CLICKHOUSE_CLIENT -q "
SELECT projections
FROM system.query_log 
WHERE query_id = '02907_test_2_$CLICKHOUSE_DATABASE' AND current_database=currentDatabase()
LIMIT 1;" | grep -o "projection_test_by_more" || true
