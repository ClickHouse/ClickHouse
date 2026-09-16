-- Tags: no-parallel-replicas, no-random-settings, no-random-merge-tree-settings

SET analyze_index_with_space_filling_curves = 1;
SET use_primary_key = 1;
SET use_skip_indexes = 1;
SET use_query_condition_cache = 0;

CREATE TABLE test_sfc_uint32_pk (x UInt32, y UInt32)
ENGINE = MergeTree ORDER BY mortonEncode(x, y)
SETTINGS index_granularity = 1;
INSERT INTO test_sfc_uint32_pk VALUES (1, 1);
SELECT countIf(explain ILIKE '%has args in%') > 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM test_sfc_uint32_pk WHERE x = 1 AND y = 1);

CREATE TABLE test_sfc_uint64_morton_pk (x UInt64, y UInt64)
ENGINE = MergeTree ORDER BY mortonEncode(x, y)
SETTINGS index_granularity = 1;
INSERT INTO test_sfc_uint64_morton_pk VALUES (4294967296, 0);
SELECT count() FROM test_sfc_uint64_morton_pk WHERE x >= toUInt64(4294967296) AND y = 0;
SELECT countIf(explain ILIKE '%has args in%') = 0
FROM (EXPLAIN indexes = 1
    SELECT count() FROM test_sfc_uint64_morton_pk WHERE x >= toUInt64(4294967296) AND y = 0);

CREATE TABLE test_sfc_uint64_hilbert_pk (x UInt64, y UInt64)
ENGINE = MergeTree ORDER BY hilbertEncode(x, y)
SETTINGS index_granularity = 1;
INSERT INTO test_sfc_uint64_hilbert_pk VALUES (4294967296, 0);
SELECT count() FROM test_sfc_uint64_hilbert_pk WHERE x >= toUInt64(4294967296) AND y = 0;
SELECT countIf(explain ILIKE '%has args in%') = 0
FROM (EXPLAIN indexes = 1
    SELECT count() FROM test_sfc_uint64_hilbert_pk WHERE x >= toUInt64(4294967296) AND y = 0);

CREATE TABLE test_sfc_uint64_morton_partition (x UInt64, y UInt64)
ENGINE = MergeTree PARTITION BY mortonEncode(x, y) ORDER BY tuple()
SETTINGS index_granularity = 1;
INSERT INTO test_sfc_uint64_morton_partition VALUES (4294967296, 0);
SELECT count() FROM test_sfc_uint64_morton_partition WHERE x >= toUInt64(4294967296) AND y = 0;

CREATE TABLE test_sfc_uint64_hilbert_partition (x UInt64, y UInt64)
ENGINE = MergeTree PARTITION BY hilbertEncode(x, y) ORDER BY tuple()
SETTINGS index_granularity = 1;
INSERT INTO test_sfc_uint64_hilbert_partition VALUES (4294967296, 0);
SELECT count() FROM test_sfc_uint64_hilbert_partition WHERE x >= toUInt64(4294967296) AND y = 0;

CREATE TABLE test_sfc_uint64_skip
(
    x UInt64,
    y UInt64,
    INDEX i_morton mortonEncode(x, y) TYPE minmax GRANULARITY 1,
    INDEX i_hilbert hilbertEncode(x, y) TYPE set(100) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 1;
INSERT INTO test_sfc_uint64_skip VALUES (4294967296, 0);
SELECT count() FROM test_sfc_uint64_skip WHERE x >= toUInt64(4294967296) AND y = 0
SETTINGS ignore_data_skipping_indices = 'i_hilbert';
SELECT countIf(explain ILIKE '%has args in%') = 0
FROM (EXPLAIN indexes = 1
    SELECT count() FROM test_sfc_uint64_skip WHERE x >= toUInt64(4294967296) AND y = 0
    SETTINGS ignore_data_skipping_indices = 'i_hilbert');
SELECT count() FROM test_sfc_uint64_skip WHERE x >= toUInt64(4294967296) AND y = 0
SETTINGS ignore_data_skipping_indices = 'i_morton';
SELECT countIf(explain ILIKE '%has args in%') = 0
FROM (EXPLAIN indexes = 1
    SELECT count() FROM test_sfc_uint64_skip WHERE x >= toUInt64(4294967296) AND y = 0
    SETTINGS ignore_data_skipping_indices = 'i_morton');

DROP TABLE test_sfc_uint32_pk, test_sfc_uint64_morton_pk, test_sfc_uint64_hilbert_pk,
    test_sfc_uint64_morton_partition, test_sfc_uint64_hilbert_partition, test_sfc_uint64_skip;
