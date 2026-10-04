-- Tags: no-random-settings, no-random-merge-tree-settings
-- EXPLAIN output may differ

-- A predicate on `y` can use a key on `ifNull(y, c)` or `coalesce(y, c)`: a row that satisfies the
-- predicate has a non-NULL `y`, for which the key is `y` itself. NULL rows only cause extra reading.

SET explain_query_plan_default = 'legacy';
SET parallel_replicas_local_plan = 1;

DROP TABLE IF EXISTS t_ifnull;
DROP TABLE IF EXISTS t_coalesce;
DROP TABLE IF EXISTS t_enum;

CREATE TABLE t_ifnull (y Nullable(UInt32))
ENGINE = MergeTree ORDER BY ifNull(y, 0) SETTINGS index_granularity = 2, auto_statistics_types = '';
INSERT INTO t_ifnull VALUES (NULL), (NULL), (1), (2), (3), (4), (5), (6), (7), (8);
OPTIMIZE TABLE t_ifnull FINAL;

CREATE TABLE t_coalesce (y Nullable(UInt32))
ENGINE = MergeTree ORDER BY coalesce(y, 0) SETTINGS index_granularity = 2, auto_statistics_types = '';
INSERT INTO t_coalesce SELECT * FROM t_ifnull;
OPTIMIZE TABLE t_coalesce FINAL;

CREATE TABLE t_enum (y Nullable(Enum8('z' = 1, 'a' = 2)))
ENGINE = MergeTree ORDER BY ifNull(y, '') SETTINGS index_granularity = 2, auto_statistics_types = '';
INSERT INTO t_enum VALUES ('a'), ('a'), ('z'), ('z');
OPTIMIZE TABLE t_enum FINAL;

-- { echo }

SELECT arraySort(groupArray(ifNull(toString(y), 'NULL'))) FROM t_ifnull WHERE y >= 5;
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT * FROM t_ifnull WHERE y >= 5) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules%';

-- The granule with the NULL rows (key 0) is a false positive.
SELECT arraySort(groupArray(ifNull(toString(y), 'NULL'))) FROM t_ifnull WHERE y < 3;
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT * FROM t_ifnull WHERE y < 3) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules%';

-- The atom is relaxed, so exact counting must not count the NULL rows.
SELECT count() FROM t_ifnull WHERE y <= 0 SETTINGS optimize_use_implicit_projections = 1;

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT * FROM t_coalesce WHERE y >= 5) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules%';

-- `ifNull` converts the `Enum` to `String`, which is ordered differently, so the key is not used.
SELECT count() FROM t_enum WHERE y > 'z';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT * FROM t_enum WHERE y > 'z') WHERE explain LIKE '%Condition%';

DROP TABLE t_ifnull;
DROP TABLE t_coalesce;
DROP TABLE t_enum;
