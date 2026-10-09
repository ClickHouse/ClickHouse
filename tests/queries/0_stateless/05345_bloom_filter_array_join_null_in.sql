-- nullIn over arrayJoin uses the bloom filter when the set has no NULL

SET explain_query_plan_default = 'legacy';
SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 0;
SET use_query_condition_cache = 0;
SET transform_null_in = 1;

DROP TABLE IF EXISTS t_bf_aj_null_in;

CREATE TABLE t_bf_aj_null_in
(
    tags Array(String),
    INDEX idx_tags tags TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 1;

-- rows 8 and 9 have empty arrays
INSERT INTO t_bf_aj_null_in SELECT if(number < 8, ['t' || toString(number)], []) FROM numbers(10);

SELECT '-- set without NULL';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_aj_null_in WHERE arrayJoin(tags) IN ('t3')) WHERE explain LIKE '%Name: idx%' OR explain LIKE '%Granules:%';
SELECT count() FROM t_bf_aj_null_in WHERE arrayJoin(tags) IN ('t3');
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_aj_null_in WHERE arrayJoin(tags) GLOBAL IN (SELECT 't3')) WHERE explain LIKE '%Name: idx%' OR explain LIKE '%Granules:%';
SELECT count() FROM t_bf_aj_null_in WHERE arrayJoin(tags) GLOBAL IN (SELECT 't3');
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_aj_null_in LEFT ARRAY JOIN tags AS tag WHERE tag GLOBAL IN (SELECT 't3')) WHERE explain LIKE '%Name: idx%' OR explain LIKE '%Granules:%';
SELECT count() FROM t_bf_aj_null_in LEFT ARRAY JOIN tags AS tag WHERE tag GLOBAL IN (SELECT 't3');

SELECT '-- a NULL in the set cannot match a String element';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_aj_null_in WHERE arrayJoin(tags) IN ('t3', NULL)) WHERE explain LIKE '%Name: idx%' OR explain LIKE '%Granules:%';
SELECT count() FROM t_bf_aj_null_in WHERE arrayJoin(tags) IN ('t3', NULL);

DROP TABLE t_bf_aj_null_in;
