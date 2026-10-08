#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel-replicas
# Tag no-fasttest: requires s3 storage
# Tag no-parallel-replicas: relies on query_log which does not account other replicas

# Test that object storage listing/reading ProfileEvents are populated

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Write two separate files so we can verify listing counts
$CLICKHOUSE_CLIENT -q "INSERT INTO FUNCTION s3('http://localhost:11111/test/${CLICKHOUSE_DATABASE}/04042_data/a.csv', 'test', 'testtest', 'CSV', 'x UInt64') SELECT number FROM numbers(50) SETTINGS s3_truncate_on_insert=1;"
$CLICKHOUSE_CLIENT -q "INSERT INTO FUNCTION s3('http://localhost:11111/test/${CLICKHOUSE_DATABASE}/04042_data/b.csv', 'test', 'testtest', 'CSV', 'x UInt64') SELECT number FROM numbers(50) SETTINGS s3_truncate_on_insert=1;"

# Read with glob pattern
query_id="SELECT_1_$RANDOM$RANDOM"
$CLICKHOUSE_CLIENT --query_id="$query_id" -q "SELECT count() FROM s3('http://localhost:11111/test/${CLICKHOUSE_DATABASE}/04042_data/*.csv', 'test', 'testtest', 'CSV', 'x UInt64') SETTINGS log_queries=1;"

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log;"

# Verify listing and reading events are present.
# ObjectStorageGlobFilteredObjects and ObjectStoragePredicateFilteredObjects count removed objects,
# which should be 0 here since all listed files match the glob and no predicate filter is applied.
$CLICKHOUSE_CLIENT -q "
SELECT
    ProfileEvents['ObjectStorageListedObjects'] AS has_listed,
    ProfileEvents['ObjectStorageGlobFilteredObjects'] AS glob_filtered,
    ProfileEvents['ObjectStoragePredicateFilteredObjects'] AS predicate_filtered,
    ProfileEvents['ObjectStorageReadObjects'] AS has_read
FROM system.query_log
WHERE
    current_database = currentDatabase()
    AND query_id = '$query_id'
    AND type = 'QueryFinish'
    AND event_date >= yesterday()
ORDER BY event_time_microseconds DESC
LIMIT 1;
"

$CLICKHOUSE_CLIENT -q "INSERT INTO FUNCTION s3('http://localhost:11111/test/${CLICKHOUSE_DATABASE}/04042_data/a_a.csv', 'test', 'testtest', 'CSV', 'x UInt64') SELECT number FROM numbers(50) SETTINGS s3_truncate_on_insert=1;"
$CLICKHOUSE_CLIENT -q "INSERT INTO FUNCTION s3('http://localhost:11111/test/${CLICKHOUSE_DATABASE}/04042_data/a_b.csv', 'test', 'testtest', 'CSV', 'x UInt64') SELECT number FROM numbers(50) SETTINGS s3_truncate_on_insert=1;"
$CLICKHOUSE_CLIENT -q "INSERT INTO FUNCTION s3('http://localhost:11111/test/${CLICKHOUSE_DATABASE}/04042_data/a_c.txt', 'test', 'testtest', 'CSV', 'x UInt64') SELECT number FROM numbers(50) SETTINGS s3_truncate_on_insert=1;"

# Read with glob pattern and predicate filter
query_id="SELECT_2_$RANDOM$RANDOM"
$CLICKHOUSE_CLIENT --query_id="$query_id" -q "SELECT count() FROM s3('http://localhost:11111/test/${CLICKHOUSE_DATABASE}/04042_data/a_*.csv', 'test', 'testtest', 'CSV', 'x UInt64') WHERE _file != 'a_a.csv' SETTINGS log_queries=1;"

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log;"

# Verify listing and reading events: glob `a_*.csv` lists 3 objects (a_a.csv, a_b.csv, a_c.txt),
# glob filtering removes a_c.txt (1), predicate `_file != 'a_a.csv'` removes a_a.csv (1), reads a_b.csv (1).
$CLICKHOUSE_CLIENT -q "
SELECT
    ProfileEvents['ObjectStorageListedObjects'] AS has_listed,
    ProfileEvents['ObjectStorageGlobFilteredObjects'] AS glob_filtered,
    ProfileEvents['ObjectStoragePredicateFilteredObjects'] AS predicate_filtered,
    ProfileEvents['ObjectStorageReadObjects'] AS has_read
FROM system.query_log
WHERE
    current_database = currentDatabase()
    AND query_id = '$query_id'
    AND type = 'QueryFinish'
    AND event_date >= yesterday()
ORDER BY event_time_microseconds DESC
LIMIT 1;
"
