-- Tags: no-random-merge-tree-settings
-- A minmax skip index on a LowCardinality(Nullable(T)) column must record NULL presence,
-- otherwise `IS NULL` wrongly prunes every granule and returns 0 rows. Issue #110037.

SET use_query_condition_cache = 0;
SET optimize_trivial_count_query = 0;
SET allow_suspicious_low_cardinality_types = 1;

DROP TABLE IF EXISTS t_lcn;

CREATE TABLE t_lcn
(
    id UInt64,
    s LowCardinality(Nullable(String)),
    INDEX s_minmax s TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 64;

INSERT INTO t_lcn
SELECT number, if(number % 17 = 0, NULL, concat('value_', toString(number)))
FROM numbers(8192)
SETTINGS max_insert_threads = 1;

-- WHERE: without and with the skip index must both be 482.
SELECT count() FROM t_lcn WHERE s IS NULL SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_lcn WHERE s IS NULL;

-- The IS NULL granules must NOT be pruned (all read).
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT * FROM t_lcn WHERE s IS NULL) WHERE explain ILIKE '%Granules: 128/128%';

-- IS NOT NULL and equality are unaffected.
SELECT count() FROM t_lcn WHERE s IS NOT NULL;
SELECT count() FROM t_lcn WHERE s = 'value_100';

-- PREWHERE is pushed to storage, so it is affected the same way.
SELECT count() FROM t_lcn PREWHERE s IS NULL SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_lcn PREWHERE s IS NULL;

-- A grouping-key HAVING predicate pushed down to storage: 1 NULL group.
SELECT count() FROM (SELECT s FROM t_lcn GROUP BY s HAVING s IS NULL) SETTINGS use_skip_indexes = 0;
SELECT count() FROM (SELECT s FROM t_lcn GROUP BY s HAVING s IS NULL);

DROP TABLE t_lcn;

-- Same bug through a Tuple subcolumn.
DROP TABLE IF EXISTS t_tuple_lcn;

CREATE TABLE t_tuple_lcn
(
    id UInt64,
    p Tuple(x LowCardinality(Nullable(String))),
    INDEX x_minmax p.x TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 64;

INSERT INTO t_tuple_lcn
SELECT number, tuple(if(number % 17 = 0, NULL, concat('value_', toString(number))))
FROM numbers(8192)
SETTINGS max_insert_threads = 1;

SELECT count() FROM t_tuple_lcn WHERE p.x IS NULL SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_tuple_lcn WHERE p.x IS NULL;

DROP TABLE t_tuple_lcn;

-- Same bug through a static JSON subcolumn.
DROP TABLE IF EXISTS t_json_lcn;

CREATE TABLE t_json_lcn
(
    id UInt64,
    data JSON(x LowCardinality(Nullable(String))),
    INDEX x_minmax data.x TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 64;

INSERT INTO t_json_lcn
SELECT number, CAST(if(number % 17 = 0, '{"x":null}', concat('{"x":"value_', toString(number), '"}')) AS JSON(x LowCardinality(Nullable(String))))
FROM numbers(8192)
SETTINGS max_insert_threads = 1;

SELECT count() FROM t_json_lcn WHERE data.x IS NULL SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_json_lcn WHERE data.x IS NULL;

DROP TABLE t_json_lcn;

-- Non-nullable LowCardinality equality pruning must still work (fix is a no-op there).
DROP TABLE IF EXISTS t_lc;

CREATE TABLE t_lc
(
    id UInt64,
    s LowCardinality(String),
    INDEX s_minmax s TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 64;

INSERT INTO t_lc SELECT number, concat('value_', toString(number)) FROM numbers(8192) SETTINGS max_insert_threads = 1;

-- An absent value is still pruned to 0 granules.
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT * FROM t_lc WHERE s = 'absent_zzz') WHERE explain ILIKE '%Granules: 0/%';

DROP TABLE t_lc;

-- Sibling code path: part/partition minmax (IMergeTreeDataPart::MinMaxIndex::update).
-- A partition-key dependency on a LowCardinality(Nullable(String)) column must keep the NULL
-- sentinel, otherwise `WHERE s IS NULL` wrongly prunes parts that contain NULLs.
DROP TABLE IF EXISTS t_part_lcn;

CREATE TABLE t_part_lcn
(
    id UInt64,
    s LowCardinality(Nullable(String))
)
ENGINE = MergeTree
ORDER BY id
PARTITION BY ifNull(s, '__none__')
SETTINGS index_granularity = 64, allow_nullable_key = 1;

INSERT INTO t_part_lcn
SELECT number, if(number % 17 = 0, NULL, concat('value_', toString(number % 3)))
FROM numbers(8192)
SETTINGS max_insert_threads = 1;

SELECT count() FROM t_part_lcn WHERE s IS NULL SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_part_lcn WHERE s IS NULL;

DROP TABLE t_part_lcn;

-- Sibling code path: set index min-max hyperrectangle (MergeTreeIndexGranuleSet).
DROP TABLE IF EXISTS t_set_lcn;

CREATE TABLE t_set_lcn
(
    id UInt64,
    s LowCardinality(Nullable(String)),
    INDEX s_set s TYPE set(0) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 64;

INSERT INTO t_set_lcn
SELECT number, if(number % 17 = 0, NULL, concat('value_', toString(number)))
FROM numbers(8192)
SETTINGS max_insert_threads = 1;

SELECT count() FROM t_set_lcn WHERE s IS NULL SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_set_lcn WHERE s IS NULL;

DROP TABLE t_set_lcn;
