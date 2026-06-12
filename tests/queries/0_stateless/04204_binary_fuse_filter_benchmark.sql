-- Tags: no-flaky-check, no-random-settings, no-random-merge-tree-settings
-- Bloom vs binary_fuse_filter size-oriented benchmark (reduced stateless coverage).

SET allow_experimental_binary_fuse_filter_index = 1;

SELECT '--- begin 04204_binary_fuse_filter_benchmark ---';

DROP TABLE IF EXISTS z42_04_bloom;
DROP TABLE IF EXISTS z42_04_binary_fuse;

-- 18 core scenarios: 6 cardinality regimes x 3 FPR values, GRANULARITY = 1.

SELECT '--- single fpr=0.025 ---';
CREATE TABLE z42_04_bloom (k UInt64, v UInt64, INDEX idx_v v TYPE bloom_filter(0.025) GRANULARITY 1) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192;
CREATE TABLE z42_04_binary_fuse (k UInt64, v UInt64, INDEX idx_v v TYPE binary_fuse_filter(0.025) GRANULARITY 1) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192;
INSERT INTO z42_04_bloom SELECT number, toUInt64(1) FROM numbers(10000);
INSERT INTO z42_04_binary_fuse SELECT number, toUInt64(1) FROM numbers(10000);
OPTIMIZE TABLE z42_04_bloom FINAL;
OPTIMIZE TABLE z42_04_binary_fuse FINAL;
SELECT 'bloom_uncompressed', sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_bloom' AND active;
SELECT 'binary_fuse_uncompressed', sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_binary_fuse' AND active;
SELECT 'bloom_compressed', sum(secondary_indices_compressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_bloom' AND active;
SELECT 'binary_fuse_compressed', sum(secondary_indices_compressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_binary_fuse' AND active;
SELECT 'skip_index_bytes_both_nonzero';
SELECT toUInt8((SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_bloom' AND active) > 0 AND (SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_binary_fuse' AND active) > 0);
DROP TABLE z42_04_bloom;
DROP TABLE z42_04_binary_fuse;

SELECT '--- single fpr=0.001 ---';
CREATE TABLE z42_04_bloom (k UInt64, v UInt64, INDEX idx_v v TYPE bloom_filter(0.001) GRANULARITY 1) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192;
CREATE TABLE z42_04_binary_fuse (k UInt64, v UInt64, INDEX idx_v v TYPE binary_fuse_filter(0.001) GRANULARITY 1) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192;
INSERT INTO z42_04_bloom SELECT number, toUInt64(1) FROM numbers(10000);
INSERT INTO z42_04_binary_fuse SELECT number, toUInt64(1) FROM numbers(10000);
OPTIMIZE TABLE z42_04_bloom FINAL;
OPTIMIZE TABLE z42_04_binary_fuse FINAL;
SELECT 'bloom_uncompressed', sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_bloom' AND active;
SELECT 'binary_fuse_uncompressed', sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_binary_fuse' AND active;
SELECT 'bloom_compressed', sum(secondary_indices_compressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_bloom' AND active;
SELECT 'binary_fuse_compressed', sum(secondary_indices_compressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_binary_fuse' AND active;
SELECT 'skip_index_bytes_both_nonzero';
SELECT toUInt8((SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_bloom' AND active) > 0 AND (SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_binary_fuse' AND active) > 0);
DROP TABLE z42_04_bloom;
DROP TABLE z42_04_binary_fuse;

SELECT '--- single fpr=0.1 ---';
CREATE TABLE z42_04_bloom (k UInt64, v UInt64, INDEX idx_v v TYPE bloom_filter(0.1) GRANULARITY 1) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192;
CREATE TABLE z42_04_binary_fuse (k UInt64, v UInt64, INDEX idx_v v TYPE binary_fuse_filter(0.1) GRANULARITY 1) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192;
INSERT INTO z42_04_bloom SELECT number, toUInt64(1) FROM numbers(10000);
INSERT INTO z42_04_binary_fuse SELECT number, toUInt64(1) FROM numbers(10000);
OPTIMIZE TABLE z42_04_bloom FINAL;
OPTIMIZE TABLE z42_04_binary_fuse FINAL;
SELECT 'bloom_uncompressed', sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_bloom' AND active;
SELECT 'binary_fuse_uncompressed', sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_binary_fuse' AND active;
SELECT 'bloom_compressed', sum(secondary_indices_compressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_bloom' AND active;
SELECT 'binary_fuse_compressed', sum(secondary_indices_compressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_binary_fuse' AND active;
SELECT 'skip_index_bytes_both_nonzero';
SELECT toUInt8((SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_bloom' AND active) > 0 AND (SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_binary_fuse' AND active) > 0);
DROP TABLE z42_04_bloom;
DROP TABLE z42_04_binary_fuse;

SELECT '--- pct1 fpr=0.025 ---';
CREATE TABLE z42_04_bloom (k UInt64, v UInt64, INDEX idx_v v TYPE bloom_filter(0.025) GRANULARITY 1) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192;
CREATE TABLE z42_04_binary_fuse (k UInt64, v UInt64, INDEX idx_v v TYPE binary_fuse_filter(0.025) GRANULARITY 1) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192;
INSERT INTO z42_04_bloom SELECT number, toUInt64(number % 100) FROM numbers(10000);
INSERT INTO z42_04_binary_fuse SELECT number, toUInt64(number % 100) FROM numbers(10000);
OPTIMIZE TABLE z42_04_bloom FINAL;
OPTIMIZE TABLE z42_04_binary_fuse FINAL;
SELECT 'bloom_uncompressed', sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_bloom' AND active;
SELECT 'binary_fuse_uncompressed', sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_binary_fuse' AND active;
SELECT 'bloom_compressed', sum(secondary_indices_compressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_bloom' AND active;
SELECT 'binary_fuse_compressed', sum(secondary_indices_compressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_binary_fuse' AND active;
SELECT 'skip_index_bytes_both_nonzero';
SELECT toUInt8((SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_bloom' AND active) > 0 AND (SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_binary_fuse' AND active) > 0);
DROP TABLE z42_04_bloom;
DROP TABLE z42_04_binary_fuse;

SELECT '--- pct1 fpr=0.001 ---';
CREATE TABLE z42_04_bloom (k UInt64, v UInt64, INDEX idx_v v TYPE bloom_filter(0.001) GRANULARITY 1) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192;
CREATE TABLE z42_04_binary_fuse (k UInt64, v UInt64, INDEX idx_v v TYPE binary_fuse_filter(0.001) GRANULARITY 1) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192;
INSERT INTO z42_04_bloom SELECT number, toUInt64(number % 100) FROM numbers(10000);
INSERT INTO z42_04_binary_fuse SELECT number, toUInt64(number % 100) FROM numbers(10000);
OPTIMIZE TABLE z42_04_bloom FINAL;
OPTIMIZE TABLE z42_04_binary_fuse FINAL;
SELECT 'bloom_uncompressed', sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_bloom' AND active;
SELECT 'binary_fuse_uncompressed', sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_binary_fuse' AND active;
SELECT 'bloom_compressed', sum(secondary_indices_compressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_bloom' AND active;
SELECT 'binary_fuse_compressed', sum(secondary_indices_compressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_binary_fuse' AND active;
SELECT 'skip_index_bytes_both_nonzero';
SELECT toUInt8((SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_bloom' AND active) > 0 AND (SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_binary_fuse' AND active) > 0);
DROP TABLE z42_04_bloom;
DROP TABLE z42_04_binary_fuse;

SELECT '--- pct1 fpr=0.1 ---';
CREATE TABLE z42_04_bloom (k UInt64, v UInt64, INDEX idx_v v TYPE bloom_filter(0.1) GRANULARITY 1) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192;
CREATE TABLE z42_04_binary_fuse (k UInt64, v UInt64, INDEX idx_v v TYPE binary_fuse_filter(0.1) GRANULARITY 1) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192;
INSERT INTO z42_04_bloom SELECT number, toUInt64(number % 100) FROM numbers(10000);
INSERT INTO z42_04_binary_fuse SELECT number, toUInt64(number % 100) FROM numbers(10000);
OPTIMIZE TABLE z42_04_bloom FINAL;
OPTIMIZE TABLE z42_04_binary_fuse FINAL;
SELECT 'bloom_uncompressed', sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_bloom' AND active;
SELECT 'binary_fuse_uncompressed', sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_binary_fuse' AND active;
SELECT 'bloom_compressed', sum(secondary_indices_compressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_bloom' AND active;
SELECT 'binary_fuse_compressed', sum(secondary_indices_compressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_binary_fuse' AND active;
SELECT 'skip_index_bytes_both_nonzero';
SELECT toUInt8((SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_bloom' AND active) > 0 AND (SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_binary_fuse' AND active) > 0);
DROP TABLE z42_04_bloom;
DROP TABLE z42_04_binary_fuse;

SELECT '--- pct10 fpr=0.025 ---';
CREATE TABLE z42_04_bloom (k UInt64, v UInt64, INDEX idx_v v TYPE bloom_filter(0.025) GRANULARITY 1) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192;
CREATE TABLE z42_04_binary_fuse (k UInt64, v UInt64, INDEX idx_v v TYPE binary_fuse_filter(0.025) GRANULARITY 1) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192;
INSERT INTO z42_04_bloom SELECT number, toUInt64(number % 1000) FROM numbers(10000);
INSERT INTO z42_04_binary_fuse SELECT number, toUInt64(number % 1000) FROM numbers(10000);
OPTIMIZE TABLE z42_04_bloom FINAL;
OPTIMIZE TABLE z42_04_binary_fuse FINAL;
SELECT 'bloom_uncompressed', sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_bloom' AND active;
SELECT 'binary_fuse_uncompressed', sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_binary_fuse' AND active;
SELECT 'bloom_compressed', sum(secondary_indices_compressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_bloom' AND active;
SELECT 'binary_fuse_compressed', sum(secondary_indices_compressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_binary_fuse' AND active;
SELECT 'skip_index_bytes_both_nonzero';
SELECT toUInt8((SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_bloom' AND active) > 0 AND (SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_binary_fuse' AND active) > 0);
DROP TABLE z42_04_bloom;
DROP TABLE z42_04_binary_fuse;

SELECT '--- pct10 fpr=0.001 ---';
CREATE TABLE z42_04_bloom (k UInt64, v UInt64, INDEX idx_v v TYPE bloom_filter(0.001) GRANULARITY 1) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192;
CREATE TABLE z42_04_binary_fuse (k UInt64, v UInt64, INDEX idx_v v TYPE binary_fuse_filter(0.001) GRANULARITY 1) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192;
INSERT INTO z42_04_bloom SELECT number, toUInt64(number % 1000) FROM numbers(10000);
INSERT INTO z42_04_binary_fuse SELECT number, toUInt64(number % 1000) FROM numbers(10000);
OPTIMIZE TABLE z42_04_bloom FINAL;
OPTIMIZE TABLE z42_04_binary_fuse FINAL;
SELECT 'bloom_uncompressed', sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_bloom' AND active;
SELECT 'binary_fuse_uncompressed', sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_binary_fuse' AND active;
SELECT 'bloom_compressed', sum(secondary_indices_compressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_bloom' AND active;
SELECT 'binary_fuse_compressed', sum(secondary_indices_compressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_binary_fuse' AND active;
SELECT 'skip_index_bytes_both_nonzero';
SELECT toUInt8((SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_bloom' AND active) > 0 AND (SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_binary_fuse' AND active) > 0);
DROP TABLE z42_04_bloom;
DROP TABLE z42_04_binary_fuse;

SELECT '--- pct10 fpr=0.1 ---';
CREATE TABLE z42_04_bloom (k UInt64, v UInt64, INDEX idx_v v TYPE bloom_filter(0.1) GRANULARITY 1) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192;
CREATE TABLE z42_04_binary_fuse (k UInt64, v UInt64, INDEX idx_v v TYPE binary_fuse_filter(0.1) GRANULARITY 1) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192;
INSERT INTO z42_04_bloom SELECT number, toUInt64(number % 1000) FROM numbers(10000);
INSERT INTO z42_04_binary_fuse SELECT number, toUInt64(number % 1000) FROM numbers(10000);
OPTIMIZE TABLE z42_04_bloom FINAL;
OPTIMIZE TABLE z42_04_binary_fuse FINAL;
SELECT 'bloom_uncompressed', sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_bloom' AND active;
SELECT 'binary_fuse_uncompressed', sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_binary_fuse' AND active;
SELECT 'bloom_compressed', sum(secondary_indices_compressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_bloom' AND active;
SELECT 'binary_fuse_compressed', sum(secondary_indices_compressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_binary_fuse' AND active;
SELECT 'skip_index_bytes_both_nonzero';
SELECT toUInt8((SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_bloom' AND active) > 0 AND (SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_binary_fuse' AND active) > 0);
DROP TABLE z42_04_bloom;
DROP TABLE z42_04_binary_fuse;

SELECT '--- pct50 fpr=0.025 ---';
CREATE TABLE z42_04_bloom (k UInt64, v UInt64, INDEX idx_v v TYPE bloom_filter(0.025) GRANULARITY 1) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192;
CREATE TABLE z42_04_binary_fuse (k UInt64, v UInt64, INDEX idx_v v TYPE binary_fuse_filter(0.025) GRANULARITY 1) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192;
INSERT INTO z42_04_bloom SELECT number, toUInt64(number % 5000) FROM numbers(10000);
INSERT INTO z42_04_binary_fuse SELECT number, toUInt64(number % 5000) FROM numbers(10000);
OPTIMIZE TABLE z42_04_bloom FINAL;
OPTIMIZE TABLE z42_04_binary_fuse FINAL;
SELECT 'bloom_uncompressed', sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_bloom' AND active;
SELECT 'binary_fuse_uncompressed', sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_binary_fuse' AND active;
SELECT 'bloom_compressed', sum(secondary_indices_compressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_bloom' AND active;
SELECT 'binary_fuse_compressed', sum(secondary_indices_compressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_binary_fuse' AND active;
SELECT 'skip_index_bytes_both_nonzero';
SELECT toUInt8((SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_bloom' AND active) > 0 AND (SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_binary_fuse' AND active) > 0);
DROP TABLE z42_04_bloom;
DROP TABLE z42_04_binary_fuse;

SELECT '--- pct50 fpr=0.001 ---';
CREATE TABLE z42_04_bloom (k UInt64, v UInt64, INDEX idx_v v TYPE bloom_filter(0.001) GRANULARITY 1) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192;
CREATE TABLE z42_04_binary_fuse (k UInt64, v UInt64, INDEX idx_v v TYPE binary_fuse_filter(0.001) GRANULARITY 1) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192;
INSERT INTO z42_04_bloom SELECT number, toUInt64(number % 5000) FROM numbers(10000);
INSERT INTO z42_04_binary_fuse SELECT number, toUInt64(number % 5000) FROM numbers(10000);
OPTIMIZE TABLE z42_04_bloom FINAL;
OPTIMIZE TABLE z42_04_binary_fuse FINAL;
SELECT 'bloom_uncompressed', sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_bloom' AND active;
SELECT 'binary_fuse_uncompressed', sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_binary_fuse' AND active;
SELECT 'bloom_compressed', sum(secondary_indices_compressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_bloom' AND active;
SELECT 'binary_fuse_compressed', sum(secondary_indices_compressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_binary_fuse' AND active;
SELECT 'skip_index_bytes_both_nonzero';
SELECT toUInt8((SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_bloom' AND active) > 0 AND (SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_binary_fuse' AND active) > 0);
DROP TABLE z42_04_bloom;
DROP TABLE z42_04_binary_fuse;

SELECT '--- pct50 fpr=0.1 ---';
CREATE TABLE z42_04_bloom (k UInt64, v UInt64, INDEX idx_v v TYPE bloom_filter(0.1) GRANULARITY 1) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192;
CREATE TABLE z42_04_binary_fuse (k UInt64, v UInt64, INDEX idx_v v TYPE binary_fuse_filter(0.1) GRANULARITY 1) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192;
INSERT INTO z42_04_bloom SELECT number, toUInt64(number % 5000) FROM numbers(10000);
INSERT INTO z42_04_binary_fuse SELECT number, toUInt64(number % 5000) FROM numbers(10000);
OPTIMIZE TABLE z42_04_bloom FINAL;
OPTIMIZE TABLE z42_04_binary_fuse FINAL;
SELECT 'bloom_uncompressed', sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_bloom' AND active;
SELECT 'binary_fuse_uncompressed', sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_binary_fuse' AND active;
SELECT 'bloom_compressed', sum(secondary_indices_compressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_bloom' AND active;
SELECT 'binary_fuse_compressed', sum(secondary_indices_compressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_binary_fuse' AND active;
SELECT 'skip_index_bytes_both_nonzero';
SELECT toUInt8((SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_bloom' AND active) > 0 AND (SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_binary_fuse' AND active) > 0);
DROP TABLE z42_04_bloom;
DROP TABLE z42_04_binary_fuse;

SELECT '--- pct90 fpr=0.025 ---';
CREATE TABLE z42_04_bloom (k UInt64, v UInt64, INDEX idx_v v TYPE bloom_filter(0.025) GRANULARITY 1) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192;
CREATE TABLE z42_04_binary_fuse (k UInt64, v UInt64, INDEX idx_v v TYPE binary_fuse_filter(0.025) GRANULARITY 1) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192;
INSERT INTO z42_04_bloom SELECT number, toUInt64(number % 9000) FROM numbers(10000);
INSERT INTO z42_04_binary_fuse SELECT number, toUInt64(number % 9000) FROM numbers(10000);
OPTIMIZE TABLE z42_04_bloom FINAL;
OPTIMIZE TABLE z42_04_binary_fuse FINAL;
SELECT 'bloom_uncompressed', sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_bloom' AND active;
SELECT 'binary_fuse_uncompressed', sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_binary_fuse' AND active;
SELECT 'bloom_compressed', sum(secondary_indices_compressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_bloom' AND active;
SELECT 'binary_fuse_compressed', sum(secondary_indices_compressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_binary_fuse' AND active;
SELECT 'skip_index_bytes_both_nonzero';
SELECT toUInt8((SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_bloom' AND active) > 0 AND (SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_binary_fuse' AND active) > 0);
DROP TABLE z42_04_bloom;
DROP TABLE z42_04_binary_fuse;

SELECT '--- pct90 fpr=0.001 ---';
CREATE TABLE z42_04_bloom (k UInt64, v UInt64, INDEX idx_v v TYPE bloom_filter(0.001) GRANULARITY 1) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192;
CREATE TABLE z42_04_binary_fuse (k UInt64, v UInt64, INDEX idx_v v TYPE binary_fuse_filter(0.001) GRANULARITY 1) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192;
INSERT INTO z42_04_bloom SELECT number, toUInt64(number % 9000) FROM numbers(10000);
INSERT INTO z42_04_binary_fuse SELECT number, toUInt64(number % 9000) FROM numbers(10000);
OPTIMIZE TABLE z42_04_bloom FINAL;
OPTIMIZE TABLE z42_04_binary_fuse FINAL;
SELECT 'bloom_uncompressed', sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_bloom' AND active;
SELECT 'binary_fuse_uncompressed', sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_binary_fuse' AND active;
SELECT 'bloom_compressed', sum(secondary_indices_compressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_bloom' AND active;
SELECT 'binary_fuse_compressed', sum(secondary_indices_compressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_binary_fuse' AND active;
SELECT 'skip_index_bytes_both_nonzero';
SELECT toUInt8((SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_bloom' AND active) > 0 AND (SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_binary_fuse' AND active) > 0);
DROP TABLE z42_04_bloom;
DROP TABLE z42_04_binary_fuse;

SELECT '--- pct90 fpr=0.1 ---';
CREATE TABLE z42_04_bloom (k UInt64, v UInt64, INDEX idx_v v TYPE bloom_filter(0.1) GRANULARITY 1) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192;
CREATE TABLE z42_04_binary_fuse (k UInt64, v UInt64, INDEX idx_v v TYPE binary_fuse_filter(0.1) GRANULARITY 1) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192;
INSERT INTO z42_04_bloom SELECT number, toUInt64(number % 9000) FROM numbers(10000);
INSERT INTO z42_04_binary_fuse SELECT number, toUInt64(number % 9000) FROM numbers(10000);
OPTIMIZE TABLE z42_04_bloom FINAL;
OPTIMIZE TABLE z42_04_binary_fuse FINAL;
SELECT 'bloom_uncompressed', sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_bloom' AND active;
SELECT 'binary_fuse_uncompressed', sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_binary_fuse' AND active;
SELECT 'bloom_compressed', sum(secondary_indices_compressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_bloom' AND active;
SELECT 'binary_fuse_compressed', sum(secondary_indices_compressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_binary_fuse' AND active;
SELECT 'skip_index_bytes_both_nonzero';
SELECT toUInt8((SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_bloom' AND active) > 0 AND (SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_binary_fuse' AND active) > 0);
DROP TABLE z42_04_bloom;
DROP TABLE z42_04_binary_fuse;

SELECT '--- alldist fpr=0.025 ---';
CREATE TABLE z42_04_bloom (k UInt64, v UInt64, INDEX idx_v v TYPE bloom_filter(0.025) GRANULARITY 1) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192;
CREATE TABLE z42_04_binary_fuse (k UInt64, v UInt64, INDEX idx_v v TYPE binary_fuse_filter(0.025) GRANULARITY 1) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192;
INSERT INTO z42_04_bloom SELECT number, number FROM numbers(10000);
INSERT INTO z42_04_binary_fuse SELECT number, number FROM numbers(10000);
OPTIMIZE TABLE z42_04_bloom FINAL;
OPTIMIZE TABLE z42_04_binary_fuse FINAL;
SELECT 'bloom_uncompressed', sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_bloom' AND active;
SELECT 'binary_fuse_uncompressed', sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_binary_fuse' AND active;
SELECT 'bloom_compressed', sum(secondary_indices_compressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_bloom' AND active;
SELECT 'binary_fuse_compressed', sum(secondary_indices_compressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_binary_fuse' AND active;
SELECT 'skip_index_bytes_both_nonzero';
SELECT toUInt8((SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_bloom' AND active) > 0 AND (SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_binary_fuse' AND active) > 0);
DROP TABLE z42_04_bloom;
DROP TABLE z42_04_binary_fuse;

SELECT '--- alldist fpr=0.001 ---';
CREATE TABLE z42_04_bloom (k UInt64, v UInt64, INDEX idx_v v TYPE bloom_filter(0.001) GRANULARITY 1) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192;
CREATE TABLE z42_04_binary_fuse (k UInt64, v UInt64, INDEX idx_v v TYPE binary_fuse_filter(0.001) GRANULARITY 1) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192;
INSERT INTO z42_04_bloom SELECT number, number FROM numbers(10000);
INSERT INTO z42_04_binary_fuse SELECT number, number FROM numbers(10000);
OPTIMIZE TABLE z42_04_bloom FINAL;
OPTIMIZE TABLE z42_04_binary_fuse FINAL;
SELECT 'bloom_uncompressed', sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_bloom' AND active;
SELECT 'binary_fuse_uncompressed', sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_binary_fuse' AND active;
SELECT 'bloom_compressed', sum(secondary_indices_compressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_bloom' AND active;
SELECT 'binary_fuse_compressed', sum(secondary_indices_compressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_binary_fuse' AND active;
SELECT 'skip_index_bytes_both_nonzero';
SELECT toUInt8((SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_bloom' AND active) > 0 AND (SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_binary_fuse' AND active) > 0);
DROP TABLE z42_04_bloom;
DROP TABLE z42_04_binary_fuse;

SELECT '--- alldist fpr=0.1 ---';
CREATE TABLE z42_04_bloom (k UInt64, v UInt64, INDEX idx_v v TYPE bloom_filter(0.1) GRANULARITY 1) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192;
CREATE TABLE z42_04_binary_fuse (k UInt64, v UInt64, INDEX idx_v v TYPE binary_fuse_filter(0.1) GRANULARITY 1) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192;
INSERT INTO z42_04_bloom SELECT number, number FROM numbers(10000);
INSERT INTO z42_04_binary_fuse SELECT number, number FROM numbers(10000);
OPTIMIZE TABLE z42_04_bloom FINAL;
OPTIMIZE TABLE z42_04_binary_fuse FINAL;
SELECT 'bloom_uncompressed', sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_bloom' AND active;
SELECT 'binary_fuse_uncompressed', sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_binary_fuse' AND active;
SELECT 'bloom_compressed', sum(secondary_indices_compressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_bloom' AND active;
SELECT 'binary_fuse_compressed', sum(secondary_indices_compressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_binary_fuse' AND active;
SELECT 'skip_index_bytes_both_nonzero';
SELECT toUInt8((SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_bloom' AND active) > 0 AND (SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_binary_fuse' AND active) > 0);
DROP TABLE z42_04_bloom;
DROP TABLE z42_04_binary_fuse;

-- Lower-FPR representative section: one lightweight case at fpr=0.005.

SELECT '--- lower-fpr representative alldist fpr=0.005 ---';
CREATE TABLE z42_04_bloom (k UInt64, v UInt64, INDEX idx_v v TYPE bloom_filter(0.005) GRANULARITY 1) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192;
CREATE TABLE z42_04_binary_fuse (k UInt64, v UInt64, INDEX idx_v v TYPE binary_fuse_filter(0.005) GRANULARITY 1) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192;
INSERT INTO z42_04_bloom SELECT number, number FROM numbers(10000);
INSERT INTO z42_04_binary_fuse SELECT number, number FROM numbers(10000);
OPTIMIZE TABLE z42_04_bloom FINAL;
OPTIMIZE TABLE z42_04_binary_fuse FINAL;
SELECT 'bloom_uncompressed', sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_bloom' AND active;
SELECT 'binary_fuse_uncompressed', sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_binary_fuse' AND active;
SELECT 'skip_index_bytes_both_nonzero';
SELECT toUInt8((SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_bloom' AND active) > 0 AND (SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_binary_fuse' AND active) > 0);
DROP TABLE z42_04_bloom;
DROP TABLE z42_04_binary_fuse;

-- GRANULARITY sensitivity (alldist, fpr=0.025): G=1, 8, 64.
-- Use index_granularity=128 so 10000 rows span many data granules (~128 rows each);
-- skip-index GRANULARITY then covers ~128 / ~1024 / ~8192 rows per index granule respectively.

SELECT '--- granularity sensitivity alldist fpr=0.025 g=1 ---';
CREATE TABLE z42_04_bloom (k UInt64, v UInt64, INDEX idx_v v TYPE bloom_filter(0.025) GRANULARITY 1) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 128;
CREATE TABLE z42_04_binary_fuse (k UInt64, v UInt64, INDEX idx_v v TYPE binary_fuse_filter(0.025) GRANULARITY 1) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 128;
INSERT INTO z42_04_bloom SELECT number, number FROM numbers(10000);
INSERT INTO z42_04_binary_fuse SELECT number, number FROM numbers(10000);
OPTIMIZE TABLE z42_04_bloom FINAL;
OPTIMIZE TABLE z42_04_binary_fuse FINAL;
SELECT 'bloom_uncompressed', sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_bloom' AND active;
SELECT 'binary_fuse_uncompressed', sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_binary_fuse' AND active;
SELECT 'skip_index_bytes_both_nonzero';
SELECT toUInt8((SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_bloom' AND active) > 0 AND (SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_binary_fuse' AND active) > 0);
DROP TABLE z42_04_bloom;
DROP TABLE z42_04_binary_fuse;

SELECT '--- granularity sensitivity alldist fpr=0.025 g=8 ---';
CREATE TABLE z42_04_bloom (k UInt64, v UInt64, INDEX idx_v v TYPE bloom_filter(0.025) GRANULARITY 8) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 128;
CREATE TABLE z42_04_binary_fuse (k UInt64, v UInt64, INDEX idx_v v TYPE binary_fuse_filter(0.025) GRANULARITY 8) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 128;
INSERT INTO z42_04_bloom SELECT number, number FROM numbers(10000);
INSERT INTO z42_04_binary_fuse SELECT number, number FROM numbers(10000);
OPTIMIZE TABLE z42_04_bloom FINAL;
OPTIMIZE TABLE z42_04_binary_fuse FINAL;
SELECT 'bloom_uncompressed', sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_bloom' AND active;
SELECT 'binary_fuse_uncompressed', sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_binary_fuse' AND active;
SELECT 'skip_index_bytes_both_nonzero';
SELECT toUInt8((SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_bloom' AND active) > 0 AND (SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_binary_fuse' AND active) > 0);
DROP TABLE z42_04_bloom;
DROP TABLE z42_04_binary_fuse;

SELECT '--- granularity sensitivity alldist fpr=0.025 g=64 ---';
CREATE TABLE z42_04_bloom (k UInt64, v UInt64, INDEX idx_v v TYPE bloom_filter(0.025) GRANULARITY 64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 128;
CREATE TABLE z42_04_binary_fuse (k UInt64, v UInt64, INDEX idx_v v TYPE binary_fuse_filter(0.025) GRANULARITY 64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 128;
INSERT INTO z42_04_bloom SELECT number, number FROM numbers(10000);
INSERT INTO z42_04_binary_fuse SELECT number, number FROM numbers(10000);
OPTIMIZE TABLE z42_04_bloom FINAL;
OPTIMIZE TABLE z42_04_binary_fuse FINAL;
SELECT 'bloom_uncompressed', sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_bloom' AND active;
SELECT 'binary_fuse_uncompressed', sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_binary_fuse' AND active;
SELECT 'skip_index_bytes_both_nonzero';
SELECT toUInt8((SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_bloom' AND active) > 0 AND (SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_binary_fuse' AND active) > 0);
DROP TABLE z42_04_bloom;
DROP TABLE z42_04_binary_fuse;

-- Distribution / locality sensitivity: Case A/B/C, lightweight (20k rows, one GRANULARITY).

SELECT '--- case A high_ndv_uniform g=8 fpr=0.025 ---';
CREATE TABLE z42_04_bloom (k UInt64, v UInt64, INDEX idx_v v TYPE bloom_filter(0.025) GRANULARITY 8) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192;
CREATE TABLE z42_04_binary_fuse (k UInt64, v UInt64, INDEX idx_v v TYPE binary_fuse_filter(0.025) GRANULARITY 8) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192;
INSERT INTO z42_04_bloom SELECT number, number FROM numbers(20000);
INSERT INTO z42_04_binary_fuse SELECT number, number FROM numbers(20000);
OPTIMIZE TABLE z42_04_bloom FINAL;
OPTIMIZE TABLE z42_04_binary_fuse FINAL;
SELECT 'bloom_uncompressed', sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_bloom' AND active;
SELECT 'binary_fuse_uncompressed', sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_binary_fuse' AND active;
DROP TABLE z42_04_bloom;
DROP TABLE z42_04_binary_fuse;

SELECT '--- case B tenant_local_low_ndv g=8 fpr=0.025 ---';
CREATE TABLE z42_04_bloom (k UInt64, v UInt64, INDEX idx_v v TYPE bloom_filter(0.025) GRANULARITY 8) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192;
CREATE TABLE z42_04_binary_fuse (k UInt64, v UInt64, INDEX idx_v v TYPE binary_fuse_filter(0.025) GRANULARITY 8) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192;
INSERT INTO z42_04_bloom SELECT number, toUInt64(intDiv(number, 4000) * 100 + (number % 32)) FROM numbers(20000);
INSERT INTO z42_04_binary_fuse SELECT number, toUInt64(intDiv(number, 4000) * 100 + (number % 32)) FROM numbers(20000);
OPTIMIZE TABLE z42_04_bloom FINAL;
OPTIMIZE TABLE z42_04_binary_fuse FINAL;
SELECT 'bloom_uncompressed', sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_bloom' AND active;
SELECT 'binary_fuse_uncompressed', sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_binary_fuse' AND active;
DROP TABLE z42_04_bloom;
DROP TABLE z42_04_binary_fuse;

SELECT '--- case C heavy_hitters_plus_rare g=8 fpr=0.025 ---';
CREATE TABLE z42_04_bloom (k UInt64, v UInt64, INDEX idx_v v TYPE bloom_filter(0.025) GRANULARITY 8) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192;
CREATE TABLE z42_04_binary_fuse (k UInt64, v UInt64, INDEX idx_v v TYPE binary_fuse_filter(0.025) GRANULARITY 8) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192;
INSERT INTO z42_04_bloom SELECT number, toUInt64(if(number % 100 < 95, number % 20, number)) FROM numbers(20000);
INSERT INTO z42_04_binary_fuse SELECT number, toUInt64(if(number % 100 < 95, number % 20, number)) FROM numbers(20000);
OPTIMIZE TABLE z42_04_bloom FINAL;
OPTIMIZE TABLE z42_04_binary_fuse FINAL;
SELECT 'bloom_uncompressed', sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_bloom' AND active;
SELECT 'binary_fuse_uncompressed', sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z42_04_binary_fuse' AND active;
DROP TABLE z42_04_bloom;
DROP TABLE z42_04_binary_fuse;

-- Tiny pruning sanity: one hit and one miss key.

SELECT '--- tiny pruning sanity alldist g=8 fpr=0.025 ---';
CREATE TABLE z42_04_bloom (k UInt64, v UInt64, INDEX idx_v v TYPE bloom_filter(0.025) GRANULARITY 8) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192;
CREATE TABLE z42_04_binary_fuse (k UInt64, v UInt64, INDEX idx_v v TYPE binary_fuse_filter(0.025) GRANULARITY 8) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192;
INSERT INTO z42_04_bloom SELECT number, number FROM numbers(10000);
INSERT INTO z42_04_binary_fuse SELECT number, number FROM numbers(10000);
OPTIMIZE TABLE z42_04_bloom FINAL;
OPTIMIZE TABLE z42_04_binary_fuse FINAL;
SELECT 'bloom_hit_granules',
    toUInt32(extract(
        (SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM z42_04_bloom PREWHERE v = toUInt64(42)) WHERE explain ILIKE '%Granules:%' LIMIT 1),
        'Granules: (\\d+)'
    ));
SELECT 'binary_fuse_hit_granules',
    toUInt32(extract(
        (SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM z42_04_binary_fuse PREWHERE v = toUInt64(42)) WHERE explain ILIKE '%Granules:%' LIMIT 1),
        'Granules: (\\d+)'
    ));
SELECT 'bloom_miss_granules',
    toUInt32(extract(
        (SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM z42_04_bloom PREWHERE v = toUInt64(10042)) WHERE explain ILIKE '%Granules:%' LIMIT 1),
        'Granules: (\\d+)'
    ));
SELECT 'binary_fuse_miss_granules',
    toUInt32(extract(
        (SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM z42_04_binary_fuse PREWHERE v = toUInt64(10042)) WHERE explain ILIKE '%Granules:%' LIMIT 1),
        'Granules: (\\d+)'
    ));
DROP TABLE z42_04_bloom;
DROP TABLE z42_04_binary_fuse;

SELECT '--- end 04204_binary_fuse_filter_benchmark ---';
