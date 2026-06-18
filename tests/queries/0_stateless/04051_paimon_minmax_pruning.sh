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

# ── Edge-case columns ────────────────────────────────────────────────────────
# The following queries exercise the correctness guards in the min-max pruner on
# columns the original dataset did not cover. Each "with pruning" query must
# return rows identical to its baseline, and enabling pruning must never turn a
# valid query into an exception. Removing the corresponding C++ guard would make
# one of these fail (wrong rows, wrong file count, or an exception).

# test 16: BIGINT > 2^31, no pruning. big_val ranges: [5e9] [6e9] [7e9].
# Predicate keeps batches 2 and 3; with pruning batch 1 is skipped (2 files).
# Guards BinaryRow::getLong reading Int64 (an Int32 read would mis-decode the bounds).
$CLICKHOUSE_CLIENT --query_id="test_04051_G_$CLICKHOUSE_TEST_UNIQUE_NAME" --session_timezone="UTC" --query "
SELECT id, big_val
FROM paimonS3(s3_conn, filename='paimon_minmax_test')
WHERE big_val > 6000000000
ORDER BY id
"
$CLICKHOUSE_CLIENT --query "select '==='"

# test 17: same BIGINT range WITH minmax pruning — must return identical rows to test 16
$CLICKHOUSE_CLIENT --query_id="test_04051_H_$CLICKHOUSE_TEST_UNIQUE_NAME" --session_timezone="UTC" --query "
SELECT id, big_val
FROM paimonS3(s3_conn, filename='paimon_minmax_test')
WHERE big_val > 6000000000
ORDER BY id
SETTINGS use_paimon_minmax_index_pruning=1
"
$CLICKHOUSE_CLIENT --query "select '==='"

# test 18: negative high-precision DECIMAL(20, 0), no pruning. dec_val ranges:
# [~-9e18] [~-5e18] [~-1e18]. Predicate keeps only batch 1; with pruning the other
# two files are skipped (1 file). Guards the sign extension in BinaryRow::getDecimal
# (zero-padding would decode the negative bounds as large positives and over-prune).
$CLICKHOUSE_CLIENT --query_id="test_04051_I_$CLICKHOUSE_TEST_UNIQUE_NAME" --session_timezone="UTC" --query "
SELECT id, dec_val
FROM paimonS3(s3_conn, filename='paimon_minmax_test')
WHERE dec_val < -6000000000000000000
ORDER BY id
"
$CLICKHOUSE_CLIENT --query "select '==='"

# test 19: same DECIMAL range WITH minmax pruning — must return identical rows to test 18
$CLICKHOUSE_CLIENT --query_id="test_04051_J_$CLICKHOUSE_TEST_UNIQUE_NAME" --session_timezone="UTC" --query "
SELECT id, dec_val
FROM paimonS3(s3_conn, filename='paimon_minmax_test')
WHERE dec_val < -6000000000000000000
ORDER BY id
SETTINGS use_paimon_minmax_index_pruning=1
"
$CLICKHOUSE_CLIENT --query "select '==='"

# test 20: IS NULL on a nullable column, no pruning. nul_val is NULL only in batch 2
# (id=4). Guards the _NULL_COUNTS check: a file whose null count is non-zero must not
# be pruned, otherwise the NULL row would be lost.
$CLICKHOUSE_CLIENT --query_id="test_04051_K_$CLICKHOUSE_TEST_UNIQUE_NAME" --session_timezone="UTC" --query "
SELECT id, nul_val
FROM paimonS3(s3_conn, filename='paimon_minmax_test')
WHERE nul_val IS NULL
ORDER BY id
"
$CLICKHOUSE_CLIENT --query "select '==='"

# test 21: same IS NULL query WITH minmax pruning — must return identical rows to test 20
$CLICKHOUSE_CLIENT --query_id="test_04051_L_$CLICKHOUSE_TEST_UNIQUE_NAME" --session_timezone="UTC" --query "
SELECT id, nul_val
FROM paimonS3(s3_conn, filename='paimon_minmax_test')
WHERE nul_val IS NULL
ORDER BY id
SETTINGS use_paimon_minmax_index_pruning=1
"
$CLICKHOUSE_CLIENT --query "select '==='"

# test 22: range query on the nullable column, no pruning. Batch 2 contains a NULL,
# so it is never pruned even when its non-null range cannot match the predicate.
$CLICKHOUSE_CLIENT --query_id="test_04051_M_$CLICKHOUSE_TEST_UNIQUE_NAME" --session_timezone="UTC" --query "
SELECT id, nul_val
FROM paimonS3(s3_conn, filename='paimon_minmax_test')
WHERE nul_val > 650
ORDER BY id
"
$CLICKHOUSE_CLIENT --query "select '==='"

# test 23: same nullable range query WITH minmax pruning — must return identical rows to test 22
$CLICKHOUSE_CLIENT --query_id="test_04051_N_$CLICKHOUSE_TEST_UNIQUE_NAME" --session_timezone="UTC" --query "
SELECT id, nul_val
FROM paimonS3(s3_conn, filename='paimon_minmax_test')
WHERE nul_val > 650
ORDER BY id
SETTINGS use_paimon_minmax_index_pruning=1
"
$CLICKHOUSE_CLIENT --query "select '==='"

# test 24: range query on TIMESTAMP(6), no pruning. ts6 has scale > 3, so its stats
# cannot be decoded and pruning is disabled for it (all files are read).
$CLICKHOUSE_CLIENT --query_id="test_04051_O_$CLICKHOUSE_TEST_UNIQUE_NAME" --session_timezone="UTC" --query "
SELECT id, ts6
FROM paimonS3(s3_conn, filename='paimon_minmax_test')
WHERE ts6 > toDateTime64('2024-06-01 00:00:00', 6, 'UTC')
ORDER BY id
"
$CLICKHOUSE_CLIENT --query "select '==='"

# test 25: same TIMESTAMP(6) query WITH minmax pruning — must return identical rows to test 24
# and must NOT throw (the scale > 3 column is skipped, not decoded).
$CLICKHOUSE_CLIENT --query_id="test_04051_P_$CLICKHOUSE_TEST_UNIQUE_NAME" --session_timezone="UTC" --query "
SELECT id, ts6
FROM paimonS3(s3_conn, filename='paimon_minmax_test')
WHERE ts6 > toDateTime64('2024-06-01 00:00:00', 6, 'UTC')
ORDER BY id
SETTINGS use_paimon_minmax_index_pruning=1
"
$CLICKHOUSE_CLIENT --query "select '==='"

# test 26: equality on a BYTES column, no pruning. BYTES is an unsupported stats type.
$CLICKHOUSE_CLIENT --query_id="test_04051_Q_$CLICKHOUSE_TEST_UNIQUE_NAME" --session_timezone="UTC" --query "
SELECT id, bin_val
FROM paimonS3(s3_conn, filename='paimon_minmax_test')
WHERE bin_val = 'bbb2'
ORDER BY id
"
$CLICKHOUSE_CLIENT --query "select '==='"

# test 27: same BYTES query WITH minmax pruning — must return identical rows to test 26
# and must NOT throw (the unsupported stats column is skipped, not decoded).
$CLICKHOUSE_CLIENT --query_id="test_04051_R_$CLICKHOUSE_TEST_UNIQUE_NAME" --session_timezone="UTC" --query "
SELECT id, bin_val
FROM paimonS3(s3_conn, filename='paimon_minmax_test')
WHERE bin_val = 'bbb2'
ORDER BY id
SETTINGS use_paimon_minmax_index_pruning=1
"
$CLICKHOUSE_CLIENT --query "select '==='"

$CLICKHOUSE_CLIENT --query "
    SYSTEM FLUSH LOGS query_log;
"

# Verify file counts: pruning queries that can prune (3,5,7,9,B,D,F on the original columns;
# H,J,L,N on big_val/dec_val/nul_val) read fewer files than their baselines. The TIMESTAMP(6)
# and BYTES pruning queries (P,R) read all files, since those columns' stats are not used.
$CLICKHOUSE_CLIENT --query "
    SELECT sum(ProfileEvents['EngineFileLikeReadFiles']) FROM system.query_log
    WHERE initial_query_id like '%test_04051%' and initial_query_id like '%$CLICKHOUSE_TEST_UNIQUE_NAME%' AND
    current_database = currentDatabase() and type='QueryFinish' group by initial_query_id order by initial_query_id;"
