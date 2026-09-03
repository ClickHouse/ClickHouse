-- Tags: long

-- Regression test for LOGICAL_ERROR 'Invalid partition key size' (STID 2677-496b).
-- A normal projection has an empty partition key, but when the parent table uses
-- part_minmax_index_columns='with_block_number_offset' the projection still gets a
-- non-empty minmax condition. During normal-projection analysis selectPartsToRead used
-- the parent part's (non-empty) partition against the projection's (empty) partition key,
-- which aborted in MergeTreePartition::getID. Constant folding must be enabled to hit the
-- per-partition condition specialization path.

DROP TABLE IF EXISTS t_04510;

CREATE TABLE t_04510 (d Date, k UInt64, v UInt64, PROJECTION p (SELECT k, v ORDER BY v))
ENGINE = MergeTree PARTITION BY toYYYYMM(d) ORDER BY k
SETTINGS index_granularity = 8192,
         enable_block_number_column = 1,
         enable_block_offset_column = 1,
         part_minmax_index_columns = 'with_block_number_offset';

-- Two partitions are enough to reach the per-partition specialization: one row inside the
-- BETWEEN range, one just outside it, and one outside the `v < 1000` range.
INSERT INTO t_04510 VALUES ('2020-01-01', 5, 10), ('2020-01-02', 6, 20), ('2020-02-01', 7, 500), ('2020-02-02', 8, 1500);

SET optimize_use_projections = 1, use_constant_folding_in_index_analysis = 1;

-- optimize_read_in_order is disabled in the forced-projection queries of this test: the base
-- table satisfies `ORDER BY k` with an in-order read, which declines the forced projection.
SELECT k FROM t_04510 WHERE v BETWEEN 10 AND 20 ORDER BY k SETTINGS force_optimize_projection = 1, optimize_read_in_order = 0;
SELECT count() FROM t_04510 WHERE v < 1000;

DROP TABLE t_04510;

-- Regression test for over-pruning of projection parts on partition virtuals.
-- generateForPart specializes the whole predicate DAG, including the _partition_id /
-- _partition_value virtuals that projection metadata still advertises from the parent partition
-- key. Feeding the projection's empty partition would fold those virtuals to 'all' / an empty
-- tuple and prune the matching projection part. Projection parts must not be specialized at all
-- (they get the unsubstituted condition), so the results below must match the non-projection
-- baseline.

DROP TABLE IF EXISTS t_04510_virt;

CREATE TABLE t_04510_virt (k UInt64, v UInt64, p UInt64, PROJECTION proj (SELECT k, v ORDER BY v))
ENGINE = MergeTree PARTITION BY p ORDER BY k;

INSERT INTO t_04510_virt VALUES (1, 15, 202001), (2, 25, 202002), (3, 12, 202001), (4, 18, 202003);

SELECT '-- _partition_id filter, no projection';
SELECT k FROM t_04510_virt WHERE _partition_id = '202001' AND v BETWEEN 10 AND 20 ORDER BY k
    SETTINGS optimize_use_projections = 0;
SELECT '-- _partition_id filter, forced projection';
SELECT k FROM t_04510_virt WHERE _partition_id = '202001' AND v BETWEEN 10 AND 20 ORDER BY k
    SETTINGS optimize_use_projections = 1, force_optimize_projection = 1, use_constant_folding_in_index_analysis = 1, optimize_read_in_order = 0;

SELECT '-- _partition_value filter, no projection';
SELECT k FROM t_04510_virt WHERE _partition_value.1 = 202002 AND v BETWEEN 20 AND 30 ORDER BY k
    SETTINGS optimize_use_projections = 0;
SELECT '-- _partition_value filter, forced projection';
SELECT k FROM t_04510_virt WHERE _partition_value.1 = 202002 AND v BETWEEN 20 AND 30 ORDER BY k
    SETTINGS optimize_use_projections = 1, force_optimize_projection = 1, use_constant_folding_in_index_analysis = 1, optimize_read_in_order = 0;

DROP TABLE t_04510_virt;

-- Regression test for wrong results with a modulo partition key.
-- The stored partition value uses the backward-compatible moduloLegacy (8-bit) result, e.g.
-- `id % 200` stores moduloLegacy(-199, 200) = 57, while the filter evaluates modulo(-199, 200)
-- = -199. Folding the stored value into the modern modulo predicate turned `id % 200 < 0` into
-- `57 < 0` and over-pruned parts. The count must match the folding-off baseline, and no parts
-- may be dropped for this whole-partition filter.
-- Only the ids carrying the divergence are needed: -199 stores in partition 57 and collides
-- there with 57, so one part mixes a negative and a positive modern modulo value, which is
-- exactly why folding a single stored value into the predicate cannot be sound.

DROP TABLE IF EXISTS t_04510_mod;

CREATE TABLE t_04510_mod (id Int64) ENGINE = MergeTree PARTITION BY id % 200 ORDER BY id;
INSERT INTO t_04510_mod VALUES (-199), (57), (-57), (-1), (3);

SELECT '-- modulo partition key, folding off';
SELECT count() FROM t_04510_mod WHERE id % 200 < 0 SETTINGS use_constant_folding_in_index_analysis = 0;
SELECT '-- modulo partition key, folding on';
SELECT count() FROM t_04510_mod WHERE id % 200 < 0 SETTINGS use_constant_folding_in_index_analysis = 1;
SELECT '-- no parts over-pruned with folding on';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_04510_mod WHERE id % 200 < 0 SETTINGS use_constant_folding_in_index_analysis = 1) WHERE explain ILIKE '%Parts: 4/4%';

-- Negative single-point = and IN also route through moduloLegacy. id = -199 stores in the
-- moduloLegacy(-199, 200) = 57 partition, colliding with id = 57. Folding on must match the
-- folding-off baseline and must not prune away that shared partition.
SELECT '-- modulo = -199, folding off';
SELECT count() FROM t_04510_mod WHERE id % 200 = -199 SETTINGS use_constant_folding_in_index_analysis = 0;
SELECT '-- modulo = -199, folding on';
SELECT count() FROM t_04510_mod WHERE id % 200 = -199 SETTINGS use_constant_folding_in_index_analysis = 1;
SELECT '-- modulo IN (-199, -57), folding off';
SELECT count() FROM t_04510_mod WHERE id % 200 IN (-199, -57) SETTINGS use_constant_folding_in_index_analysis = 0;
SELECT '-- modulo IN (-199, -57), folding on';
SELECT count() FROM t_04510_mod WHERE id % 200 IN (-199, -57) SETTINGS use_constant_folding_in_index_analysis = 1;

DROP TABLE t_04510_mod;

-- Regression test for a data race / crash with a stateful sparseGrams tokenizer.
-- With folding on, the skip-index condition is regenerated per partition in parallel, and every
-- build shared the text index's single sparseGrams tokenizer whose iterator state is mutable.
-- Concurrent tokenization of the LIKE needle corrupted that shared state (heap corruption / SIGSEGV
-- in SparseGramsTokenizer::nextInStringLike). Each condition must own a private clone of a stateful
-- tokenizer. Many partitions + max_threads > 1 are required to run the per-partition builds in
-- parallel. The result must be stable across repetitions.

DROP TABLE IF EXISTS t_04510_sg;

CREATE TABLE t_04510_sg
(
    id UInt32,
    s String,
    INDEX idx s TYPE text(tokenizer = sparseGrams(3, 20, 5)) GRANULARITY 1
)
ENGINE = MergeTree PARTITION BY id ORDER BY id SETTINGS index_granularity = 1;

INSERT INTO t_04510_sg SELECT number, 'foobar' || toString(number) FROM numbers(64);

-- The condition is built at two different call sites depending on
-- use_skip_indexes_on_data_read, which the test runner randomizes, so pin both explicitly
-- instead of leaving one of the two paths untested at random.
-- force_data_skipping_indices asserts index use on the executing query itself, so each route
-- carries its own assertion. EXPLAIN cannot do this: it resets use_skip_indexes_on_data_read
-- to false for every non-ANALYZE kind, so an EXPLAIN guard only ever observes the = 0 route.
SELECT '-- sparseGrams LIKE under parallel per-partition folding, initial analysis';
SELECT count() FROM t_04510_sg WHERE s LIKE '%a%'
    SETTINGS use_constant_folding_in_index_analysis = 1, max_threads = 16, use_skip_indexes_on_data_read = 0, force_data_skipping_indices = 'idx';
SELECT count() FROM t_04510_sg WHERE s LIKE '%a%'
    SETTINGS use_constant_folding_in_index_analysis = 1, max_threads = 16, use_skip_indexes_on_data_read = 0, force_data_skipping_indices = 'idx';
SELECT count() FROM t_04510_sg WHERE s LIKE '%a%'
    SETTINGS use_constant_folding_in_index_analysis = 1, max_threads = 16, use_skip_indexes_on_data_read = 0, force_data_skipping_indices = 'idx';
SELECT '-- sparseGrams LIKE under parallel per-partition folding, data read';
SELECT count() FROM t_04510_sg WHERE s LIKE '%a%'
    SETTINGS use_constant_folding_in_index_analysis = 1, max_threads = 16, use_skip_indexes_on_data_read = 1, force_data_skipping_indices = 'idx';
SELECT count() FROM t_04510_sg WHERE s LIKE '%a%'
    SETTINGS use_constant_folding_in_index_analysis = 1, max_threads = 16, use_skip_indexes_on_data_read = 1, force_data_skipping_indices = 'idx';
SELECT count() FROM t_04510_sg WHERE s LIKE '%a%'
    SETTINGS use_constant_folding_in_index_analysis = 1, max_threads = 16, use_skip_indexes_on_data_read = 1, force_data_skipping_indices = 'idx';

DROP TABLE t_04510_sg;

-- The legacy bloom-filter text index reaches the same stateful tokenizer through a separate
-- consumer (MergeTreeConditionBloomFilterText), so it needs its own coverage: the `sparse_grams`
-- index type is the only way to select that tokenizer there.

DROP TABLE IF EXISTS t_04510_bf;

CREATE TABLE t_04510_bf
(
    id UInt32,
    s String,
    INDEX idx s TYPE sparse_grams(3, 20, 256, 2, 0) GRANULARITY 1
)
ENGINE = MergeTree PARTITION BY id ORDER BY id SETTINGS index_granularity = 1;

INSERT INTO t_04510_bf SELECT number, 'foobar' || toString(number) FROM numbers(64);

SELECT '-- sparse_grams bloom filter LIKE under parallel per-partition folding, initial analysis';
SELECT count() FROM t_04510_bf WHERE s LIKE '%a%'
    SETTINGS use_constant_folding_in_index_analysis = 1, max_threads = 16, use_skip_indexes_on_data_read = 0, force_data_skipping_indices = 'idx';
SELECT count() FROM t_04510_bf WHERE s LIKE '%a%'
    SETTINGS use_constant_folding_in_index_analysis = 1, max_threads = 16, use_skip_indexes_on_data_read = 0, force_data_skipping_indices = 'idx';
SELECT count() FROM t_04510_bf WHERE s LIKE '%a%'
    SETTINGS use_constant_folding_in_index_analysis = 1, max_threads = 16, use_skip_indexes_on_data_read = 0, force_data_skipping_indices = 'idx';
SELECT '-- sparse_grams bloom filter LIKE under parallel per-partition folding, data read';
SELECT count() FROM t_04510_bf WHERE s LIKE '%a%'
    SETTINGS use_constant_folding_in_index_analysis = 1, max_threads = 16, use_skip_indexes_on_data_read = 1, force_data_skipping_indices = 'idx';
SELECT count() FROM t_04510_bf WHERE s LIKE '%a%'
    SETTINGS use_constant_folding_in_index_analysis = 1, max_threads = 16, use_skip_indexes_on_data_read = 1, force_data_skipping_indices = 'idx';
SELECT count() FROM t_04510_bf WHERE s LIKE '%a%'
    SETTINGS use_constant_folding_in_index_analysis = 1, max_threads = 16, use_skip_indexes_on_data_read = 1, force_data_skipping_indices = 'idx';

DROP TABLE t_04510_bf;
