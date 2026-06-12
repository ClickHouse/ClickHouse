-- Tags: no-flaky-check, no-parallel, no-random-settings, no-random-merge-tree-settings
-- Bloom vs cuckoo_filter bytes comparison in six cardinality regimes and three FPR values.

SET allow_experimental_cuckoo_filter_index = 1;

SELECT '--- begin 04104_cuckoo_filter_benchmark ---';

-- single
SELECT '--- single fpr=0.025 ---';
DROP TABLE IF EXISTS z41_04_bloom;
DROP TABLE IF EXISTS z41_04_cuckoo;
CREATE TABLE z41_04_bloom (`k` UInt64, `v` UInt64, INDEX idx_v v TYPE bloom_filter(0.025) GRANULARITY 1) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192;
CREATE TABLE z41_04_cuckoo (`k` UInt64, `v` UInt64, INDEX idx_v v TYPE cuckoo_filter(0.025) GRANULARITY 1) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192;
INSERT INTO z41_04_bloom SELECT number, toUInt64(1) FROM numbers(10000);
INSERT INTO z41_04_cuckoo SELECT number, toUInt64(1) FROM numbers(10000);
OPTIMIZE TABLE z41_04_bloom FINAL;
OPTIMIZE TABLE z41_04_cuckoo FINAL;
SELECT 'bloom_uncompressed', sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_bloom' AND active;
SELECT 'cuckoo_uncompressed', sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_cuckoo' AND active;
SELECT 'bloom_compressed', sum(secondary_indices_compressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_bloom' AND active;
SELECT 'cuckoo_compressed', sum(secondary_indices_compressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_cuckoo' AND active;
SELECT 'skip_index_bytes_both_nonzero';
SELECT toUInt8((SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_bloom' AND active) > 0 AND (SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_cuckoo' AND active) > 0);
SELECT 'cuckoo_not_larger_than_bloom';
SELECT toUInt8(
    (SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_cuckoo' AND active)
    <=
    (SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_bloom' AND active)
);




DROP TABLE IF EXISTS z41_04_bloom;
DROP TABLE IF EXISTS z41_04_cuckoo;
SELECT '--- single fpr=0.001 ---';
DROP TABLE IF EXISTS z41_04_bloom;
DROP TABLE IF EXISTS z41_04_cuckoo;
CREATE TABLE z41_04_bloom (`k` UInt64, `v` UInt64, INDEX idx_v v TYPE bloom_filter(0.001) GRANULARITY 1) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192;
CREATE TABLE z41_04_cuckoo (`k` UInt64, `v` UInt64, INDEX idx_v v TYPE cuckoo_filter(0.001) GRANULARITY 1) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192;
INSERT INTO z41_04_bloom SELECT number, toUInt64(1) FROM numbers(10000);
INSERT INTO z41_04_cuckoo SELECT number, toUInt64(1) FROM numbers(10000);
OPTIMIZE TABLE z41_04_bloom FINAL;
OPTIMIZE TABLE z41_04_cuckoo FINAL;
SELECT 'bloom_uncompressed', sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_bloom' AND active;
SELECT 'cuckoo_uncompressed', sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_cuckoo' AND active;
SELECT 'bloom_compressed', sum(secondary_indices_compressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_bloom' AND active;
SELECT 'cuckoo_compressed', sum(secondary_indices_compressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_cuckoo' AND active;
SELECT 'skip_index_bytes_both_nonzero';
SELECT toUInt8((SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_bloom' AND active) > 0 AND (SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_cuckoo' AND active) > 0);
SELECT 'cuckoo_not_larger_than_bloom';
SELECT toUInt8(
    (SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_cuckoo' AND active)
    <=
    (SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_bloom' AND active)
);




DROP TABLE IF EXISTS z41_04_bloom;
DROP TABLE IF EXISTS z41_04_cuckoo;
SELECT '--- single fpr=0.1 ---';
DROP TABLE IF EXISTS z41_04_bloom;
DROP TABLE IF EXISTS z41_04_cuckoo;
CREATE TABLE z41_04_bloom (`k` UInt64, `v` UInt64, INDEX idx_v v TYPE bloom_filter(0.1) GRANULARITY 1) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192;
CREATE TABLE z41_04_cuckoo (`k` UInt64, `v` UInt64, INDEX idx_v v TYPE cuckoo_filter(0.1) GRANULARITY 1) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192;
INSERT INTO z41_04_bloom SELECT number, toUInt64(1) FROM numbers(10000);
INSERT INTO z41_04_cuckoo SELECT number, toUInt64(1) FROM numbers(10000);
OPTIMIZE TABLE z41_04_bloom FINAL;
OPTIMIZE TABLE z41_04_cuckoo FINAL;
SELECT 'bloom_uncompressed', sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_bloom' AND active;
SELECT 'cuckoo_uncompressed', sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_cuckoo' AND active;
SELECT 'bloom_compressed', sum(secondary_indices_compressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_bloom' AND active;
SELECT 'cuckoo_compressed', sum(secondary_indices_compressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_cuckoo' AND active;
SELECT 'skip_index_bytes_both_nonzero';
SELECT toUInt8((SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_bloom' AND active) > 0 AND (SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_cuckoo' AND active) > 0);
SELECT 'cuckoo_not_larger_than_bloom';
SELECT toUInt8(
    (SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_cuckoo' AND active)
    <=
    (SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_bloom' AND active)
);




DROP TABLE IF EXISTS z41_04_bloom;
DROP TABLE IF EXISTS z41_04_cuckoo;

-- pct1
SELECT '--- pct1 fpr=0.025 ---';
DROP TABLE IF EXISTS z41_04_bloom;
DROP TABLE IF EXISTS z41_04_cuckoo;
CREATE TABLE z41_04_bloom (`k` UInt64, `v` UInt64, INDEX idx_v v TYPE bloom_filter(0.025) GRANULARITY 1) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192;
CREATE TABLE z41_04_cuckoo (`k` UInt64, `v` UInt64, INDEX idx_v v TYPE cuckoo_filter(0.025) GRANULARITY 1) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192;
INSERT INTO z41_04_bloom SELECT number, toUInt64(number % 100) FROM numbers(10000);
INSERT INTO z41_04_cuckoo SELECT number, toUInt64(number % 100) FROM numbers(10000);
OPTIMIZE TABLE z41_04_bloom FINAL;
OPTIMIZE TABLE z41_04_cuckoo FINAL;
SELECT 'bloom_uncompressed', sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_bloom' AND active;
SELECT 'cuckoo_uncompressed', sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_cuckoo' AND active;
SELECT 'bloom_compressed', sum(secondary_indices_compressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_bloom' AND active;
SELECT 'cuckoo_compressed', sum(secondary_indices_compressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_cuckoo' AND active;
SELECT 'skip_index_bytes_both_nonzero';
SELECT toUInt8((SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_bloom' AND active) > 0 AND (SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_cuckoo' AND active) > 0);
SELECT 'cuckoo_not_larger_than_bloom';
SELECT toUInt8(
    (SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_cuckoo' AND active)
    <=
    (SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_bloom' AND active)
);




DROP TABLE IF EXISTS z41_04_bloom;
DROP TABLE IF EXISTS z41_04_cuckoo;
SELECT '--- pct1 fpr=0.001 ---';
DROP TABLE IF EXISTS z41_04_bloom;
DROP TABLE IF EXISTS z41_04_cuckoo;
CREATE TABLE z41_04_bloom (`k` UInt64, `v` UInt64, INDEX idx_v v TYPE bloom_filter(0.001) GRANULARITY 1) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192;
CREATE TABLE z41_04_cuckoo (`k` UInt64, `v` UInt64, INDEX idx_v v TYPE cuckoo_filter(0.001) GRANULARITY 1) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192;
INSERT INTO z41_04_bloom SELECT number, toUInt64(number % 100) FROM numbers(10000);
INSERT INTO z41_04_cuckoo SELECT number, toUInt64(number % 100) FROM numbers(10000);
OPTIMIZE TABLE z41_04_bloom FINAL;
OPTIMIZE TABLE z41_04_cuckoo FINAL;
SELECT 'bloom_uncompressed', sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_bloom' AND active;
SELECT 'cuckoo_uncompressed', sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_cuckoo' AND active;
SELECT 'bloom_compressed', sum(secondary_indices_compressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_bloom' AND active;
SELECT 'cuckoo_compressed', sum(secondary_indices_compressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_cuckoo' AND active;
SELECT 'skip_index_bytes_both_nonzero';
SELECT toUInt8((SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_bloom' AND active) > 0 AND (SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_cuckoo' AND active) > 0);
SELECT 'cuckoo_not_larger_than_bloom';
SELECT toUInt8(
    (SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_cuckoo' AND active)
    <=
    (SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_bloom' AND active)
);




DROP TABLE IF EXISTS z41_04_bloom;
DROP TABLE IF EXISTS z41_04_cuckoo;
SELECT '--- pct1 fpr=0.1 ---';
DROP TABLE IF EXISTS z41_04_bloom;
DROP TABLE IF EXISTS z41_04_cuckoo;
CREATE TABLE z41_04_bloom (`k` UInt64, `v` UInt64, INDEX idx_v v TYPE bloom_filter(0.1) GRANULARITY 1) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192;
CREATE TABLE z41_04_cuckoo (`k` UInt64, `v` UInt64, INDEX idx_v v TYPE cuckoo_filter(0.1) GRANULARITY 1) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192;
INSERT INTO z41_04_bloom SELECT number, toUInt64(number % 100) FROM numbers(10000);
INSERT INTO z41_04_cuckoo SELECT number, toUInt64(number % 100) FROM numbers(10000);
OPTIMIZE TABLE z41_04_bloom FINAL;
OPTIMIZE TABLE z41_04_cuckoo FINAL;
SELECT 'bloom_uncompressed', sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_bloom' AND active;
SELECT 'cuckoo_uncompressed', sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_cuckoo' AND active;
SELECT 'bloom_compressed', sum(secondary_indices_compressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_bloom' AND active;
SELECT 'cuckoo_compressed', sum(secondary_indices_compressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_cuckoo' AND active;
SELECT 'skip_index_bytes_both_nonzero';
SELECT toUInt8((SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_bloom' AND active) > 0 AND (SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_cuckoo' AND active) > 0);
SELECT 'cuckoo_not_larger_than_bloom';
SELECT toUInt8(
    (SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_cuckoo' AND active)
    <=
    (SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_bloom' AND active)
);




DROP TABLE IF EXISTS z41_04_bloom;
DROP TABLE IF EXISTS z41_04_cuckoo;

-- pct10
SELECT '--- pct10 fpr=0.025 ---';
DROP TABLE IF EXISTS z41_04_bloom;
DROP TABLE IF EXISTS z41_04_cuckoo;
CREATE TABLE z41_04_bloom (`k` UInt64, `v` UInt64, INDEX idx_v v TYPE bloom_filter(0.025) GRANULARITY 1) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192;
CREATE TABLE z41_04_cuckoo (`k` UInt64, `v` UInt64, INDEX idx_v v TYPE cuckoo_filter(0.025) GRANULARITY 1) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192;
INSERT INTO z41_04_bloom SELECT number, toUInt64(number % 1000) FROM numbers(10000);
INSERT INTO z41_04_cuckoo SELECT number, toUInt64(number % 1000) FROM numbers(10000);
OPTIMIZE TABLE z41_04_bloom FINAL;
OPTIMIZE TABLE z41_04_cuckoo FINAL;
SELECT 'bloom_uncompressed', sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_bloom' AND active;
SELECT 'cuckoo_uncompressed', sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_cuckoo' AND active;
SELECT 'bloom_compressed', sum(secondary_indices_compressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_bloom' AND active;
SELECT 'cuckoo_compressed', sum(secondary_indices_compressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_cuckoo' AND active;
SELECT 'skip_index_bytes_both_nonzero';
SELECT toUInt8((SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_bloom' AND active) > 0 AND (SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_cuckoo' AND active) > 0);
SELECT 'cuckoo_not_larger_than_bloom';
SELECT toUInt8(
    (SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_cuckoo' AND active)
    <=
    (SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_bloom' AND active)
);




DROP TABLE IF EXISTS z41_04_bloom;
DROP TABLE IF EXISTS z41_04_cuckoo;
SELECT '--- pct10 fpr=0.001 ---';
DROP TABLE IF EXISTS z41_04_bloom;
DROP TABLE IF EXISTS z41_04_cuckoo;
CREATE TABLE z41_04_bloom (`k` UInt64, `v` UInt64, INDEX idx_v v TYPE bloom_filter(0.001) GRANULARITY 1) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192;
CREATE TABLE z41_04_cuckoo (`k` UInt64, `v` UInt64, INDEX idx_v v TYPE cuckoo_filter(0.001) GRANULARITY 1) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192;
INSERT INTO z41_04_bloom SELECT number, toUInt64(number % 1000) FROM numbers(10000);
INSERT INTO z41_04_cuckoo SELECT number, toUInt64(number % 1000) FROM numbers(10000);
OPTIMIZE TABLE z41_04_bloom FINAL;
OPTIMIZE TABLE z41_04_cuckoo FINAL;
SELECT 'bloom_uncompressed', sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_bloom' AND active;
SELECT 'cuckoo_uncompressed', sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_cuckoo' AND active;
SELECT 'bloom_compressed', sum(secondary_indices_compressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_bloom' AND active;
SELECT 'cuckoo_compressed', sum(secondary_indices_compressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_cuckoo' AND active;
SELECT 'skip_index_bytes_both_nonzero';
SELECT toUInt8((SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_bloom' AND active) > 0 AND (SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_cuckoo' AND active) > 0);
SELECT 'cuckoo_not_larger_than_bloom';
SELECT toUInt8(
    (SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_cuckoo' AND active)
    <=
    (SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_bloom' AND active)
);




DROP TABLE IF EXISTS z41_04_bloom;
DROP TABLE IF EXISTS z41_04_cuckoo;
SELECT '--- pct10 fpr=0.1 ---';
DROP TABLE IF EXISTS z41_04_bloom;
DROP TABLE IF EXISTS z41_04_cuckoo;
CREATE TABLE z41_04_bloom (`k` UInt64, `v` UInt64, INDEX idx_v v TYPE bloom_filter(0.1) GRANULARITY 1) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192;
CREATE TABLE z41_04_cuckoo (`k` UInt64, `v` UInt64, INDEX idx_v v TYPE cuckoo_filter(0.1) GRANULARITY 1) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192;
INSERT INTO z41_04_bloom SELECT number, toUInt64(number % 1000) FROM numbers(10000);
INSERT INTO z41_04_cuckoo SELECT number, toUInt64(number % 1000) FROM numbers(10000);
OPTIMIZE TABLE z41_04_bloom FINAL;
OPTIMIZE TABLE z41_04_cuckoo FINAL;
SELECT 'bloom_uncompressed', sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_bloom' AND active;
SELECT 'cuckoo_uncompressed', sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_cuckoo' AND active;
SELECT 'bloom_compressed', sum(secondary_indices_compressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_bloom' AND active;
SELECT 'cuckoo_compressed', sum(secondary_indices_compressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_cuckoo' AND active;
SELECT 'skip_index_bytes_both_nonzero';
SELECT toUInt8((SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_bloom' AND active) > 0 AND (SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_cuckoo' AND active) > 0);
SELECT 'cuckoo_not_larger_than_bloom';
SELECT toUInt8(
    (SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_cuckoo' AND active)
    <=
    (SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_bloom' AND active)
);




DROP TABLE IF EXISTS z41_04_bloom;
DROP TABLE IF EXISTS z41_04_cuckoo;

-- pct50
SELECT '--- pct50 fpr=0.025 ---';
DROP TABLE IF EXISTS z41_04_bloom;
DROP TABLE IF EXISTS z41_04_cuckoo;
CREATE TABLE z41_04_bloom (`k` UInt64, `v` UInt64, INDEX idx_v v TYPE bloom_filter(0.025) GRANULARITY 1) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192;
CREATE TABLE z41_04_cuckoo (`k` UInt64, `v` UInt64, INDEX idx_v v TYPE cuckoo_filter(0.025) GRANULARITY 1) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192;
INSERT INTO z41_04_bloom SELECT number, toUInt64(number % 5000) FROM numbers(10000);
INSERT INTO z41_04_cuckoo SELECT number, toUInt64(number % 5000) FROM numbers(10000);
OPTIMIZE TABLE z41_04_bloom FINAL;
OPTIMIZE TABLE z41_04_cuckoo FINAL;
SELECT 'bloom_uncompressed', sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_bloom' AND active;
SELECT 'cuckoo_uncompressed', sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_cuckoo' AND active;
SELECT 'bloom_compressed', sum(secondary_indices_compressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_bloom' AND active;
SELECT 'cuckoo_compressed', sum(secondary_indices_compressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_cuckoo' AND active;
SELECT 'skip_index_bytes_both_nonzero';
SELECT toUInt8((SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_bloom' AND active) > 0 AND (SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_cuckoo' AND active) > 0);
SELECT 'cuckoo_not_larger_than_bloom';
SELECT toUInt8(
    (SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_cuckoo' AND active)
    <=
    (SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_bloom' AND active)
);




DROP TABLE IF EXISTS z41_04_bloom;
DROP TABLE IF EXISTS z41_04_cuckoo;
SELECT '--- pct50 fpr=0.001 ---';
DROP TABLE IF EXISTS z41_04_bloom;
DROP TABLE IF EXISTS z41_04_cuckoo;
CREATE TABLE z41_04_bloom (`k` UInt64, `v` UInt64, INDEX idx_v v TYPE bloom_filter(0.001) GRANULARITY 1) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192;
CREATE TABLE z41_04_cuckoo (`k` UInt64, `v` UInt64, INDEX idx_v v TYPE cuckoo_filter(0.001) GRANULARITY 1) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192;
INSERT INTO z41_04_bloom SELECT number, toUInt64(number % 5000) FROM numbers(10000);
INSERT INTO z41_04_cuckoo SELECT number, toUInt64(number % 5000) FROM numbers(10000);
OPTIMIZE TABLE z41_04_bloom FINAL;
OPTIMIZE TABLE z41_04_cuckoo FINAL;
SELECT 'bloom_uncompressed', sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_bloom' AND active;
SELECT 'cuckoo_uncompressed', sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_cuckoo' AND active;
SELECT 'bloom_compressed', sum(secondary_indices_compressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_bloom' AND active;
SELECT 'cuckoo_compressed', sum(secondary_indices_compressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_cuckoo' AND active;
SELECT 'skip_index_bytes_both_nonzero';
SELECT toUInt8((SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_bloom' AND active) > 0 AND (SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_cuckoo' AND active) > 0);
SELECT 'cuckoo_not_larger_than_bloom';
SELECT toUInt8(
    (SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_cuckoo' AND active)
    <=
    (SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_bloom' AND active)
);




DROP TABLE IF EXISTS z41_04_bloom;
DROP TABLE IF EXISTS z41_04_cuckoo;
SELECT '--- pct50 fpr=0.1 ---';
DROP TABLE IF EXISTS z41_04_bloom;
DROP TABLE IF EXISTS z41_04_cuckoo;
CREATE TABLE z41_04_bloom (`k` UInt64, `v` UInt64, INDEX idx_v v TYPE bloom_filter(0.1) GRANULARITY 1) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192;
CREATE TABLE z41_04_cuckoo (`k` UInt64, `v` UInt64, INDEX idx_v v TYPE cuckoo_filter(0.1) GRANULARITY 1) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192;
INSERT INTO z41_04_bloom SELECT number, toUInt64(number % 5000) FROM numbers(10000);
INSERT INTO z41_04_cuckoo SELECT number, toUInt64(number % 5000) FROM numbers(10000);
OPTIMIZE TABLE z41_04_bloom FINAL;
OPTIMIZE TABLE z41_04_cuckoo FINAL;
SELECT 'bloom_uncompressed', sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_bloom' AND active;
SELECT 'cuckoo_uncompressed', sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_cuckoo' AND active;
SELECT 'bloom_compressed', sum(secondary_indices_compressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_bloom' AND active;
SELECT 'cuckoo_compressed', sum(secondary_indices_compressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_cuckoo' AND active;
SELECT 'skip_index_bytes_both_nonzero';
SELECT toUInt8((SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_bloom' AND active) > 0 AND (SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_cuckoo' AND active) > 0);
SELECT 'cuckoo_not_larger_than_bloom';
SELECT toUInt8(
    (SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_cuckoo' AND active)
    <=
    (SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_bloom' AND active)
);




DROP TABLE IF EXISTS z41_04_bloom;
DROP TABLE IF EXISTS z41_04_cuckoo;

-- pct90
SELECT '--- pct90 fpr=0.025 ---';
DROP TABLE IF EXISTS z41_04_bloom;
DROP TABLE IF EXISTS z41_04_cuckoo;
CREATE TABLE z41_04_bloom (`k` UInt64, `v` UInt64, INDEX idx_v v TYPE bloom_filter(0.025) GRANULARITY 1) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192;
CREATE TABLE z41_04_cuckoo (`k` UInt64, `v` UInt64, INDEX idx_v v TYPE cuckoo_filter(0.025) GRANULARITY 1) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192;
INSERT INTO z41_04_bloom SELECT number, toUInt64(number % 9000) FROM numbers(10000);
INSERT INTO z41_04_cuckoo SELECT number, toUInt64(number % 9000) FROM numbers(10000);
OPTIMIZE TABLE z41_04_bloom FINAL;
OPTIMIZE TABLE z41_04_cuckoo FINAL;
SELECT 'bloom_uncompressed', sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_bloom' AND active;
SELECT 'cuckoo_uncompressed', sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_cuckoo' AND active;
SELECT 'bloom_compressed', sum(secondary_indices_compressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_bloom' AND active;
SELECT 'cuckoo_compressed', sum(secondary_indices_compressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_cuckoo' AND active;
SELECT 'skip_index_bytes_both_nonzero';
SELECT toUInt8((SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_bloom' AND active) > 0 AND (SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_cuckoo' AND active) > 0);
SELECT 'cuckoo_not_larger_than_bloom';
SELECT toUInt8(
    (SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_cuckoo' AND active)
    <=
    (SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_bloom' AND active)
);




DROP TABLE IF EXISTS z41_04_bloom;
DROP TABLE IF EXISTS z41_04_cuckoo;
SELECT '--- pct90 fpr=0.001 ---';
DROP TABLE IF EXISTS z41_04_bloom;
DROP TABLE IF EXISTS z41_04_cuckoo;
CREATE TABLE z41_04_bloom (`k` UInt64, `v` UInt64, INDEX idx_v v TYPE bloom_filter(0.001) GRANULARITY 1) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192;
CREATE TABLE z41_04_cuckoo (`k` UInt64, `v` UInt64, INDEX idx_v v TYPE cuckoo_filter(0.001) GRANULARITY 1) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192;
INSERT INTO z41_04_bloom SELECT number, toUInt64(number % 9000) FROM numbers(10000);
INSERT INTO z41_04_cuckoo SELECT number, toUInt64(number % 9000) FROM numbers(10000);
OPTIMIZE TABLE z41_04_bloom FINAL;
OPTIMIZE TABLE z41_04_cuckoo FINAL;
SELECT 'bloom_uncompressed', sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_bloom' AND active;
SELECT 'cuckoo_uncompressed', sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_cuckoo' AND active;
SELECT 'bloom_compressed', sum(secondary_indices_compressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_bloom' AND active;
SELECT 'cuckoo_compressed', sum(secondary_indices_compressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_cuckoo' AND active;
SELECT 'skip_index_bytes_both_nonzero';
SELECT toUInt8((SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_bloom' AND active) > 0 AND (SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_cuckoo' AND active) > 0);
SELECT 'cuckoo_not_larger_than_bloom';
SELECT toUInt8(
    (SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_cuckoo' AND active)
    <=
    (SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_bloom' AND active)
);




DROP TABLE IF EXISTS z41_04_bloom;
DROP TABLE IF EXISTS z41_04_cuckoo;
SELECT '--- pct90 fpr=0.1 ---';
DROP TABLE IF EXISTS z41_04_bloom;
DROP TABLE IF EXISTS z41_04_cuckoo;
CREATE TABLE z41_04_bloom (`k` UInt64, `v` UInt64, INDEX idx_v v TYPE bloom_filter(0.1) GRANULARITY 1) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192;
CREATE TABLE z41_04_cuckoo (`k` UInt64, `v` UInt64, INDEX idx_v v TYPE cuckoo_filter(0.1) GRANULARITY 1) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192;
INSERT INTO z41_04_bloom SELECT number, toUInt64(number % 9000) FROM numbers(10000);
INSERT INTO z41_04_cuckoo SELECT number, toUInt64(number % 9000) FROM numbers(10000);
OPTIMIZE TABLE z41_04_bloom FINAL;
OPTIMIZE TABLE z41_04_cuckoo FINAL;
SELECT 'bloom_uncompressed', sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_bloom' AND active;
SELECT 'cuckoo_uncompressed', sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_cuckoo' AND active;
SELECT 'bloom_compressed', sum(secondary_indices_compressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_bloom' AND active;
SELECT 'cuckoo_compressed', sum(secondary_indices_compressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_cuckoo' AND active;
SELECT 'skip_index_bytes_both_nonzero';
SELECT toUInt8((SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_bloom' AND active) > 0 AND (SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_cuckoo' AND active) > 0);
SELECT 'cuckoo_not_larger_than_bloom';
SELECT toUInt8(
    (SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_cuckoo' AND active)
    <=
    (SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_bloom' AND active)
);




DROP TABLE IF EXISTS z41_04_bloom;
DROP TABLE IF EXISTS z41_04_cuckoo;

-- alldist
SELECT '--- alldist fpr=0.025 ---';
DROP TABLE IF EXISTS z41_04_bloom;
DROP TABLE IF EXISTS z41_04_cuckoo;
CREATE TABLE z41_04_bloom (`k` UInt64, `v` UInt64, INDEX idx_v v TYPE bloom_filter(0.025) GRANULARITY 1) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192;
CREATE TABLE z41_04_cuckoo (`k` UInt64, `v` UInt64, INDEX idx_v v TYPE cuckoo_filter(0.025) GRANULARITY 1) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192;
INSERT INTO z41_04_bloom SELECT number, toUInt64(number) FROM numbers(10000);
INSERT INTO z41_04_cuckoo SELECT number, toUInt64(number) FROM numbers(10000);
OPTIMIZE TABLE z41_04_bloom FINAL;
OPTIMIZE TABLE z41_04_cuckoo FINAL;
SELECT 'bloom_uncompressed', sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_bloom' AND active;
SELECT 'cuckoo_uncompressed', sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_cuckoo' AND active;
SELECT 'bloom_compressed', sum(secondary_indices_compressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_bloom' AND active;
SELECT 'cuckoo_compressed', sum(secondary_indices_compressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_cuckoo' AND active;
SELECT 'skip_index_bytes_both_nonzero';
SELECT toUInt8((SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_bloom' AND active) > 0 AND (SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_cuckoo' AND active) > 0);
SELECT 'cuckoo_not_larger_than_bloom';
SELECT toUInt8(
    (SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_cuckoo' AND active)
    <=
    (SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_bloom' AND active)
);




DROP TABLE IF EXISTS z41_04_bloom;
DROP TABLE IF EXISTS z41_04_cuckoo;
SELECT '--- alldist fpr=0.001 ---';
DROP TABLE IF EXISTS z41_04_bloom;
DROP TABLE IF EXISTS z41_04_cuckoo;
CREATE TABLE z41_04_bloom (`k` UInt64, `v` UInt64, INDEX idx_v v TYPE bloom_filter(0.001) GRANULARITY 1) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192;
CREATE TABLE z41_04_cuckoo (`k` UInt64, `v` UInt64, INDEX idx_v v TYPE cuckoo_filter(0.001) GRANULARITY 1) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192;
INSERT INTO z41_04_bloom SELECT number, toUInt64(number) FROM numbers(10000);
INSERT INTO z41_04_cuckoo SELECT number, toUInt64(number) FROM numbers(10000);
OPTIMIZE TABLE z41_04_bloom FINAL;
OPTIMIZE TABLE z41_04_cuckoo FINAL;
SELECT 'bloom_uncompressed', sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_bloom' AND active;
SELECT 'cuckoo_uncompressed', sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_cuckoo' AND active;
SELECT 'bloom_compressed', sum(secondary_indices_compressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_bloom' AND active;
SELECT 'cuckoo_compressed', sum(secondary_indices_compressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_cuckoo' AND active;
SELECT 'skip_index_bytes_both_nonzero';
SELECT toUInt8((SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_bloom' AND active) > 0 AND (SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_cuckoo' AND active) > 0);
SELECT 'cuckoo_not_larger_than_bloom';
SELECT toUInt8(
    (SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_cuckoo' AND active)
    <=
    (SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_bloom' AND active)
);




DROP TABLE IF EXISTS z41_04_bloom;
DROP TABLE IF EXISTS z41_04_cuckoo;
SELECT '--- alldist fpr=0.1 ---';
DROP TABLE IF EXISTS z41_04_bloom;
DROP TABLE IF EXISTS z41_04_cuckoo;
CREATE TABLE z41_04_bloom (`k` UInt64, `v` UInt64, INDEX idx_v v TYPE bloom_filter(0.1) GRANULARITY 1) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192;
CREATE TABLE z41_04_cuckoo (`k` UInt64, `v` UInt64, INDEX idx_v v TYPE cuckoo_filter(0.1) GRANULARITY 1) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192;
INSERT INTO z41_04_bloom SELECT number, toUInt64(number) FROM numbers(10000);
INSERT INTO z41_04_cuckoo SELECT number, toUInt64(number) FROM numbers(10000);
OPTIMIZE TABLE z41_04_bloom FINAL;
OPTIMIZE TABLE z41_04_cuckoo FINAL;
SELECT 'bloom_uncompressed', sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_bloom' AND active;
SELECT 'cuckoo_uncompressed', sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_cuckoo' AND active;
SELECT 'bloom_compressed', sum(secondary_indices_compressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_bloom' AND active;
SELECT 'cuckoo_compressed', sum(secondary_indices_compressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_cuckoo' AND active;
SELECT 'skip_index_bytes_both_nonzero';
SELECT toUInt8((SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_bloom' AND active) > 0 AND (SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_cuckoo' AND active) > 0);
SELECT 'cuckoo_not_larger_than_bloom';
SELECT toUInt8(
    (SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_cuckoo' AND active)
    <=
    (SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'z41_04_bloom' AND active)
);




DROP TABLE IF EXISTS z41_04_bloom;
DROP TABLE IF EXISTS z41_04_cuckoo;

SELECT '--- end 04104_cuckoo_filter_benchmark ---';
