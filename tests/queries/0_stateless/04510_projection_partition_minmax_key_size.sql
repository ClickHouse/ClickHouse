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

INSERT INTO t_04510 SELECT toDate('2020-01-01') + (number % 400), number, number * 2 FROM numbers(5000);

SET optimize_use_projections = 1, use_constant_folding_in_index_analysis = 1;

SELECT k FROM t_04510 WHERE v BETWEEN 10 AND 20 ORDER BY k SETTINGS force_optimize_projection = 1;
SELECT count() FROM t_04510 WHERE v < 1000;

DROP TABLE t_04510;

-- Regression test for over-pruning of projection parts on partition virtuals.
-- generateForPartition specializes the whole predicate DAG, including the _partition_id /
-- _partition_value virtuals that projection metadata still advertises from the parent partition
-- key. Feeding the projection's empty partition would fold those virtuals to 'all' / an empty
-- tuple and prune the matching projection part. The projection read must use the parent part's
-- real partition for these virtuals, so the results below must match the non-projection baseline.

DROP TABLE IF EXISTS t_04510_virt;

CREATE TABLE t_04510_virt (k UInt64, v UInt64, p UInt64, PROJECTION proj (SELECT k, v ORDER BY v))
ENGINE = MergeTree PARTITION BY p ORDER BY k;

INSERT INTO t_04510_virt VALUES (1, 15, 202001), (2, 25, 202002), (3, 12, 202001), (4, 18, 202003);

SELECT '-- _partition_id filter, no projection';
SELECT k FROM t_04510_virt WHERE _partition_id = '202001' AND v BETWEEN 10 AND 20 ORDER BY k
    SETTINGS optimize_use_projections = 0;
SELECT '-- _partition_id filter, forced projection';
SELECT k FROM t_04510_virt WHERE _partition_id = '202001' AND v BETWEEN 10 AND 20 ORDER BY k
    SETTINGS optimize_use_projections = 1, force_optimize_projection = 1, use_constant_folding_in_index_analysis = 1;

SELECT '-- _partition_value filter, no projection';
SELECT k FROM t_04510_virt WHERE _partition_value.1 = 202002 AND v BETWEEN 20 AND 30 ORDER BY k
    SETTINGS optimize_use_projections = 0;
SELECT '-- _partition_value filter, forced projection';
SELECT k FROM t_04510_virt WHERE _partition_value.1 = 202002 AND v BETWEEN 20 AND 30 ORDER BY k
    SETTINGS optimize_use_projections = 1, force_optimize_projection = 1, use_constant_folding_in_index_analysis = 1;

DROP TABLE t_04510_virt;
