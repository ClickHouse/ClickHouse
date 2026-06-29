#!/usr/bin/env bash
# Tags: no-fasttest,no-parallel-replicas
# no-parallel-replicas: the ProfileEvents with the expected values are reported on the replicas the query runs in,
# and the coordinator does not collect all ProfileEvents values.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# All queries below are issued in a single clickhouse-client invocation. Running each query in its
# own client process made the test exceed the 180s limit in instrumented builds, because every
# invocation pays the clickhouse-client startup cost. Each query carries a distinct `log_comment`
# so the per-query file-count check at the end can attribute `EngineFileLikeReadFiles` to it.
#
# Each "with pruning" query must return rows identical to its baseline, and enabling pruning must
# never turn a valid query into an exception. The edge-case columns (big_val, dec_val, nul_val,
# ts6, bin_val) exercise the correctness guards in the min-max pruner; removing the corresponding
# C++ guard would make one of these fail (wrong rows, wrong file count, or an exception).

U="$CLICKHOUSE_TEST_UNIQUE_NAME"

$CLICKHOUSE_CLIENT --session_timezone="UTC" --query "
-- test 1: full scan baseline (no pruning)
SELECT id, int_val, str_val, ts1, ts3 FROM paimonS3(s3_conn, filename='paimon_minmax_test') ORDER BY id SETTINGS log_comment='test_04051_1_${U}';
SELECT '===';
-- test 2: range query on value column, no minmax pruning
SELECT id, int_val, str_val, ts1, ts3 FROM paimonS3(s3_conn, filename='paimon_minmax_test') WHERE int_val > 100 ORDER BY id SETTINGS log_comment='test_04051_2_${U}';
SELECT '===';
-- test 3: same range query WITH minmax pruning -- must return identical rows to test 2
SELECT id, int_val, str_val, ts1, ts3 FROM paimonS3(s3_conn, filename='paimon_minmax_test') WHERE int_val > 100 ORDER BY id SETTINGS use_paimon_minmax_index_pruning=1, log_comment='test_04051_3_${U}';
SELECT '===';
-- test 4: more selective range query, no minmax pruning
SELECT id, int_val, str_val, ts1, ts3 FROM paimonS3(s3_conn, filename='paimon_minmax_test') WHERE int_val > 200 ORDER BY id SETTINGS log_comment='test_04051_4_${U}';
SELECT '===';
-- test 5: same selective range query WITH minmax pruning -- must return identical rows to test 4
SELECT id, int_val, str_val, ts1, ts3 FROM paimonS3(s3_conn, filename='paimon_minmax_test') WHERE int_val > 200 ORDER BY id SETTINGS use_paimon_minmax_index_pruning=1, log_comment='test_04051_5_${U}';
SELECT '===';
-- test 6: equality predicate, no pruning
SELECT id, int_val, str_val, ts1, ts3 FROM paimonS3(s3_conn, filename='paimon_minmax_test') WHERE int_val = 120 ORDER BY id SETTINGS log_comment='test_04051_6_${U}';
SELECT '===';
-- test 7: same equality predicate WITH minmax pruning -- must return identical rows to test 6
SELECT id, int_val, str_val, ts1, ts3 FROM paimonS3(s3_conn, filename='paimon_minmax_test') WHERE int_val = 120 ORDER BY id SETTINGS use_paimon_minmax_index_pruning=1, log_comment='test_04051_7_${U}';
SELECT '===';
-- test 8: range query on ts3, no minmax pruning. With pruning only 2 files need to be read.
SELECT id, int_val, str_val, ts1, ts3 FROM paimonS3(s3_conn, filename='paimon_minmax_test') WHERE ts3 > toDateTime64('2024-06-01 00:00:00', 3, 'UTC') ORDER BY id SETTINGS log_comment='test_04051_8_${U}';
SELECT '===';
-- test 9: same range query on ts3 WITH minmax pruning -- identical rows to test 8
SELECT id, int_val, str_val, ts1, ts3 FROM paimonS3(s3_conn, filename='paimon_minmax_test') WHERE ts3 > toDateTime64('2024-06-01 00:00:00', 3, 'UTC') ORDER BY id SETTINGS use_paimon_minmax_index_pruning=1, log_comment='test_04051_9_${U}';
SELECT '===';
-- test 10: more selective range query on ts3, no minmax pruning. With pruning only 1 file (batch 3).
SELECT id, int_val, str_val, ts1, ts3 FROM paimonS3(s3_conn, filename='paimon_minmax_test') WHERE ts3 > toDateTime64('2024-07-01 00:00:00', 3, 'UTC') ORDER BY id SETTINGS log_comment='test_04051_A_${U}';
SELECT '===';
-- test 11: same selective range query on ts3 WITH minmax pruning -- identical rows to test 10
SELECT id, int_val, str_val, ts1, ts3 FROM paimonS3(s3_conn, filename='paimon_minmax_test') WHERE ts3 > toDateTime64('2024-07-01 00:00:00', 3, 'UTC') ORDER BY id SETTINGS use_paimon_minmax_index_pruning=1, log_comment='test_04051_B_${U}';
SELECT '===';
-- test 12: range query on ts1, no minmax pruning. With pruning only 2 files need to be read.
SELECT id, int_val, str_val, ts1, ts3 FROM paimonS3(s3_conn, filename='paimon_minmax_test') WHERE ts1 > toDateTime64('2024-06-01 00:00:00', 1, 'UTC') ORDER BY id SETTINGS log_comment='test_04051_C_${U}';
SELECT '===';
-- test 13: same range query on ts1 WITH minmax pruning -- identical rows to test 12
SELECT id, int_val, str_val, ts1, ts3 FROM paimonS3(s3_conn, filename='paimon_minmax_test') WHERE ts1 > toDateTime64('2024-06-01 00:00:00', 1, 'UTC') ORDER BY id SETTINGS use_paimon_minmax_index_pruning=1, log_comment='test_04051_D_${U}';
SELECT '===';
-- test 14: more selective range query on ts1, no minmax pruning. With pruning only 1 file (batch 3).
SELECT id, int_val, str_val, ts1, ts3 FROM paimonS3(s3_conn, filename='paimon_minmax_test') WHERE ts1 > toDateTime64('2024-07-01 00:00:00', 1, 'UTC') ORDER BY id SETTINGS log_comment='test_04051_E_${U}';
SELECT '===';
-- test 15: same selective range query on ts1 WITH minmax pruning -- identical rows to test 14
SELECT id, int_val, str_val, ts1, ts3 FROM paimonS3(s3_conn, filename='paimon_minmax_test') WHERE ts1 > toDateTime64('2024-07-01 00:00:00', 1, 'UTC') ORDER BY id SETTINGS use_paimon_minmax_index_pruning=1, log_comment='test_04051_F_${U}';
SELECT '===';
-- test 16: BIGINT > 2^31, no pruning. Guards BinaryRow::getLong reading Int64.
SELECT id, big_val FROM paimonS3(s3_conn, filename='paimon_minmax_test') WHERE big_val > 6000000000 ORDER BY id SETTINGS log_comment='test_04051_G_${U}';
SELECT '===';
-- test 17: same BIGINT range WITH minmax pruning -- identical rows to test 16
SELECT id, big_val FROM paimonS3(s3_conn, filename='paimon_minmax_test') WHERE big_val > 6000000000 ORDER BY id SETTINGS use_paimon_minmax_index_pruning=1, log_comment='test_04051_H_${U}';
SELECT '===';
-- test 18: negative high-precision DECIMAL(20, 0), no pruning. Guards sign extension in BinaryRow::getDecimal.
SELECT id, dec_val FROM paimonS3(s3_conn, filename='paimon_minmax_test') WHERE dec_val < -6000000000000000000 ORDER BY id SETTINGS log_comment='test_04051_I_${U}';
SELECT '===';
-- test 19: same DECIMAL range WITH minmax pruning -- identical rows to test 18
SELECT id, dec_val FROM paimonS3(s3_conn, filename='paimon_minmax_test') WHERE dec_val < -6000000000000000000 ORDER BY id SETTINGS use_paimon_minmax_index_pruning=1, log_comment='test_04051_J_${U}';
SELECT '===';
-- test 20: IS NULL on a nullable column, no pruning. Guards the _NULL_COUNTS check.
SELECT id, nul_val FROM paimonS3(s3_conn, filename='paimon_minmax_test') WHERE nul_val IS NULL ORDER BY id SETTINGS log_comment='test_04051_K_${U}';
SELECT '===';
-- test 21: same IS NULL query WITH minmax pruning -- identical rows to test 20
SELECT id, nul_val FROM paimonS3(s3_conn, filename='paimon_minmax_test') WHERE nul_val IS NULL ORDER BY id SETTINGS use_paimon_minmax_index_pruning=1, log_comment='test_04051_L_${U}';
SELECT '===';
-- test 22: range query on the nullable column, no pruning. Batch 2 contains a NULL so it is never pruned.
SELECT id, nul_val FROM paimonS3(s3_conn, filename='paimon_minmax_test') WHERE nul_val > 650 ORDER BY id SETTINGS log_comment='test_04051_M_${U}';
SELECT '===';
-- test 23: same nullable range query WITH minmax pruning -- identical rows to test 22
SELECT id, nul_val FROM paimonS3(s3_conn, filename='paimon_minmax_test') WHERE nul_val > 650 ORDER BY id SETTINGS use_paimon_minmax_index_pruning=1, log_comment='test_04051_N_${U}';
SELECT '===';
-- test 24: range query on TIMESTAMP(6), no pruning. scale > 3 stats cannot be decoded, pruning disabled for it.
SELECT id, ts6 FROM paimonS3(s3_conn, filename='paimon_minmax_test') WHERE ts6 > toDateTime64('2024-06-01 00:00:00', 6, 'UTC') ORDER BY id SETTINGS log_comment='test_04051_O_${U}';
SELECT '===';
-- test 25: same TIMESTAMP(6) query WITH minmax pruning -- identical rows to test 24, must NOT throw.
SELECT id, ts6 FROM paimonS3(s3_conn, filename='paimon_minmax_test') WHERE ts6 > toDateTime64('2024-06-01 00:00:00', 6, 'UTC') ORDER BY id SETTINGS use_paimon_minmax_index_pruning=1, log_comment='test_04051_P_${U}';
SELECT '===';
-- test 26: equality on a BYTES column, no pruning. BYTES is an unsupported stats type.
SELECT id, bin_val FROM paimonS3(s3_conn, filename='paimon_minmax_test') WHERE bin_val = 'bbb2' ORDER BY id SETTINGS log_comment='test_04051_Q_${U}';
SELECT '===';
-- test 27: same BYTES query WITH minmax pruning -- identical rows to test 26, must NOT throw.
SELECT id, bin_val FROM paimonS3(s3_conn, filename='paimon_minmax_test') WHERE bin_val = 'bbb2' ORDER BY id SETTINGS use_paimon_minmax_index_pruning=1, log_comment='test_04051_R_${U}';
SELECT '===';
SYSTEM FLUSH LOGS query_log;
-- Per-query file counts: pruning queries that can prune read fewer files than their baselines.
-- The TIMESTAMP(6) and BYTES pruning queries read all files, since those columns' stats are not used.
SELECT sum(ProfileEvents['EngineFileLikeReadFiles']) FROM system.query_log
WHERE log_comment LIKE 'test_04051%' AND log_comment LIKE '%${U}%'
AND current_database = currentDatabase() AND type = 'QueryFinish'
GROUP BY log_comment ORDER BY log_comment;
"
