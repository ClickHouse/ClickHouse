#!/usr/bin/env bash
# Tags: no-fasttest,no-parallel-replicas
# no-parallel-replicas: the ProfileEvents with the expected values are reported on the replicas the query runs in,
# and the coordinator does not collect all ProfileEvents values.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DATA_PATH="${CURDIR}/data_minio/paimon_minmax_test"

# test 1: full scan baseline (no pruning)
$CLICKHOUSE_CLIENT --query_id="test_04051_1_$CLICKHOUSE_TEST_UNIQUE_NAME" --session_timezone="UTC" --query "
SELECT id, int_val, str_val, ts1, ts3
FROM paimonS3(s3_conn, filename='paimon_minmax_test')
ORDER BY id
"
$CLICKHOUSE_CLIENT --query "select '==='"

# test 2: range query on value column, no minmax pruning
$CLICKHOUSE_CLIENT --query_id="test_04051_2_$CLICKHOUSE_TEST_UNIQUE_NAME" --session_timezone="UTC" --query "
SELECT id, int_val, str_val, ts1, ts3
FROM paimonS3(s3_conn, filename='paimon_minmax_test')
WHERE int_val > 100
ORDER BY id
"
$CLICKHOUSE_CLIENT --query "select '==='"

# test 3: same range query WITH minmax pruning — must return identical rows to test 2
$CLICKHOUSE_CLIENT --query_id="test_04051_3_$CLICKHOUSE_TEST_UNIQUE_NAME" --session_timezone="UTC" --query "
SELECT id, int_val, str_val, ts1, ts3
FROM paimonS3(s3_conn, filename='paimon_minmax_test')
WHERE int_val > 100
ORDER BY id
SETTINGS use_paimon_minmax_index_pruning=1
"
$CLICKHOUSE_CLIENT --query "select '==='"

# test 4: more selective range query, no minmax pruning
$CLICKHOUSE_CLIENT --query_id="test_04051_4_$CLICKHOUSE_TEST_UNIQUE_NAME" --session_timezone="UTC" --query "
SELECT id, int_val, str_val, ts1, ts3
FROM paimonS3(s3_conn, filename='paimon_minmax_test')
WHERE int_val > 200
ORDER BY id
"
$CLICKHOUSE_CLIENT --query "select '==='"

# test 5: same selective range query WITH minmax pruning — must return identical rows to test 4
$CLICKHOUSE_CLIENT --query_id="test_04051_5_$CLICKHOUSE_TEST_UNIQUE_NAME" --session_timezone="UTC" --query "
SELECT id, int_val, str_val, ts1, ts3
FROM paimonS3(s3_conn, filename='paimon_minmax_test')
WHERE int_val > 200
ORDER BY id
SETTINGS use_paimon_minmax_index_pruning=1
"
$CLICKHOUSE_CLIENT --query "select '==='"

# test 6: equality predicate, no pruning
$CLICKHOUSE_CLIENT --query_id="test_04051_6_$CLICKHOUSE_TEST_UNIQUE_NAME" --session_timezone="UTC" --query "
SELECT id, int_val, str_val, ts1, ts3
FROM paimonS3(s3_conn, filename='paimon_minmax_test')
WHERE int_val = 120
ORDER BY id
"
$CLICKHOUSE_CLIENT --query "select '==='"

# test 7: same equality predicate WITH minmax pruning — must return identical rows to test 6
$CLICKHOUSE_CLIENT --query_id="test_04051_7_$CLICKHOUSE_TEST_UNIQUE_NAME" --session_timezone="UTC" --query "
SELECT id, int_val, str_val, ts1, ts3
FROM paimonS3(s3_conn, filename='paimon_minmax_test')
WHERE int_val = 120
ORDER BY id
SETTINGS use_paimon_minmax_index_pruning=1
"
$CLICKHOUSE_CLIENT --query "select '==='"

# test 8: range query on ts3, no minmax pruning
# Batch 1 max ts3 = 2024-01-01 12:00:00 which is before the predicate threshold,
# so with pruning only 2 files need to be read.
$CLICKHOUSE_CLIENT --query_id="test_04051_8_$CLICKHOUSE_TEST_UNIQUE_NAME" --session_timezone="UTC" --query "
SELECT id, int_val, str_val, ts1, ts3
FROM paimonS3(s3_conn, filename='paimon_minmax_test')
WHERE ts3 > toDateTime64('2024-06-01 00:00:00', 3, 'UTC')
ORDER BY id
"
$CLICKHOUSE_CLIENT --query "select '==='"

# test 9: same range query on ts3 WITH minmax pruning — must return identical rows to test 8
$CLICKHOUSE_CLIENT --query_id="test_04051_9_$CLICKHOUSE_TEST_UNIQUE_NAME" --session_timezone="UTC" --query "
SELECT id, int_val, str_val, ts1, ts3
FROM paimonS3(s3_conn, filename='paimon_minmax_test')
WHERE ts3 > toDateTime64('2024-06-01 00:00:00', 3, 'UTC')
ORDER BY id
SETTINGS use_paimon_minmax_index_pruning=1
"
$CLICKHOUSE_CLIENT --query "select '==='"

# test 10: more selective range query on ts3, no minmax pruning
# Batch 1 max = 2024-01-01 12:00:00 and batch 2 max = 2024-06-15 20:00:00, both before 2024-07-01,
# so with pruning only 1 file (batch 3) needs to be read.
$CLICKHOUSE_CLIENT --query_id="test_04051_A_$CLICKHOUSE_TEST_UNIQUE_NAME" --session_timezone="UTC" --query "
SELECT id, int_val, str_val, ts1, ts3
FROM paimonS3(s3_conn, filename='paimon_minmax_test')
WHERE ts3 > toDateTime64('2024-07-01 00:00:00', 3, 'UTC')
ORDER BY id
"
$CLICKHOUSE_CLIENT --query "select '==='"

# test 11: same selective range query on ts3 WITH minmax pruning — must return identical rows to test 10
$CLICKHOUSE_CLIENT --query_id="test_04051_B_$CLICKHOUSE_TEST_UNIQUE_NAME" --session_timezone="UTC" --query "
SELECT id, int_val, str_val, ts1, ts3
FROM paimonS3(s3_conn, filename='paimon_minmax_test')
WHERE ts3 > toDateTime64('2024-07-01 00:00:00', 3, 'UTC')
ORDER BY id
SETTINGS use_paimon_minmax_index_pruning=1
"
$CLICKHOUSE_CLIENT --query "select '==='"

# test 12: range query on ts1, no minmax pruning
# Batch 1 max ts1 = 2024-01-01 12:00:00 which is before the predicate threshold,
# so with pruning only 2 files need to be read.
$CLICKHOUSE_CLIENT --query_id="test_04051_C_$CLICKHOUSE_TEST_UNIQUE_NAME" --session_timezone="UTC" --query "
SELECT id, int_val, str_val, ts1, ts3
FROM paimonS3(s3_conn, filename='paimon_minmax_test')
WHERE ts1 > toDateTime64('2024-06-01 00:00:00', 1, 'UTC')
ORDER BY id
"
$CLICKHOUSE_CLIENT --query "select '==='"

# test 13: same range query on ts1 WITH minmax pruning — must return identical rows to test 12
$CLICKHOUSE_CLIENT --query_id="test_04051_D_$CLICKHOUSE_TEST_UNIQUE_NAME" --session_timezone="UTC" --query "
SELECT id, int_val, str_val, ts1, ts3
FROM paimonS3(s3_conn, filename='paimon_minmax_test')
WHERE ts1 > toDateTime64('2024-06-01 00:00:00', 1, 'UTC')
ORDER BY id
SETTINGS use_paimon_minmax_index_pruning=1
"
$CLICKHOUSE_CLIENT --query "select '==='"

# test 14: more selective range query on ts1, no minmax pruning
# Batch 1 max = 2024-01-01 12:00:00 and batch 2 max = 2024-06-15 20:00:00, both before 2024-07-01,
# so with pruning only 1 file (batch 3) needs to be read.
$CLICKHOUSE_CLIENT --query_id="test_04051_E_$CLICKHOUSE_TEST_UNIQUE_NAME" --session_timezone="UTC" --query "
SELECT id, int_val, str_val, ts1, ts3
FROM paimonS3(s3_conn, filename='paimon_minmax_test')
WHERE ts1 > toDateTime64('2024-07-01 00:00:00', 1, 'UTC')
ORDER BY id
"
$CLICKHOUSE_CLIENT --query "select '==='"

# test 15: same selective range query on ts1 WITH minmax pruning — must return identical rows to test 14
$CLICKHOUSE_CLIENT --query_id="test_04051_F_$CLICKHOUSE_TEST_UNIQUE_NAME" --session_timezone="UTC" --query "
SELECT id, int_val, str_val, ts1, ts3
FROM paimonS3(s3_conn, filename='paimon_minmax_test')
WHERE ts1 > toDateTime64('2024-07-01 00:00:00', 1, 'UTC')
ORDER BY id
SETTINGS use_paimon_minmax_index_pruning=1
"
$CLICKHOUSE_CLIENT --query "select '==='"

$CLICKHOUSE_CLIENT --query "
    SYSTEM FLUSH LOGS query_log;
"

# Verify file counts: pruning queries (3,5,7,9,B,D,F) should read fewer files than their baselines (2,4,6,8,A,C,E)
$CLICKHOUSE_CLIENT --query "
    SELECT sum(ProfileEvents['EngineFileLikeReadFiles']) FROM system.query_log
    WHERE initial_query_id like '%test_04051%' and initial_query_id like '%$CLICKHOUSE_TEST_UNIQUE_NAME%' AND
    current_database = currentDatabase() and type='QueryFinish' group by initial_query_id order by initial_query_id;"
