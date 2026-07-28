-- Tags: no-parallel-replicas

-- Documents the interaction of `use_constant_folding_in_index_analysis` with the
-- `force_primary_key` / `force_index_by_date` settings.
--
-- Per-partition constant folding is data dependent: it can make the primary key or the
-- MinMax index effective only after per-part specialization, while the `force_*` checks are
-- evaluated against the unsubstituted condition. As a result the query is pruned at run time,
-- but the `force_*` settings still report `INDEX_NOT_USED`. These tests lock in that behavior
-- so a future change does not silently alter it.

SET enable_analyzer = 1;
SET use_constant_folding_in_index_analysis = 1;
SET use_statistics_for_part_pruning=0;

DROP TABLE IF EXISTS folding_force_pk;

CREATE TABLE folding_force_pk (p UInt64, b UInt64)
ENGINE = MergeTree
ORDER BY b
PARTITION BY p
SETTINGS index_granularity = 1;

INSERT INTO folding_force_pk VALUES (1, 10), (1, 11), (2, 20);

-- Constant folding prunes granules using the primary key per partition.
SELECT 'force_primary_key result';
SELECT * FROM folding_force_pk
WHERE p != 1 OR b = 10
ORDER BY p, b
SETTINGS use_skip_indexes = 0;

-- ... yet force_primary_key still throws because the unsubstituted condition is broad.
SELECT * FROM folding_force_pk
WHERE p != 1 OR b = 10
SETTINGS use_skip_indexes = 0, force_primary_key = 1; -- { serverError INDEX_NOT_USED }

DROP TABLE folding_force_pk;

DROP TABLE IF EXISTS folding_force_minmax;

CREATE TABLE folding_force_minmax (a UInt64, b UInt64)
ENGINE = MergeTree
ORDER BY b
PARTITION BY a
SETTINGS index_granularity = 1;

INSERT INTO folding_force_minmax VALUES (1, 10), (2, 20);

-- Constant folding prunes parts using the partition MinMax index.
SELECT 'force_index_by_date result';
SELECT * FROM folding_force_minmax
WHERE if(a = 1, 1, 0) = 1
ORDER BY a, b;

-- ... yet force_index_by_date still throws because the unsubstituted condition is broad.
SELECT * FROM folding_force_minmax
WHERE if(a = 1, 1, 0) = 1
SETTINGS force_index_by_date = 1; -- { serverError INDEX_NOT_USED }

DROP TABLE folding_force_minmax;
