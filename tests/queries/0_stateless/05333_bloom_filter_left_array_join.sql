-- LEFT ARRAY JOIN uses the bloom filter unless the filter accepts the default element

SET explain_query_plan_default = 'legacy';
SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 0;
SET use_query_condition_cache = 0;

DROP TABLE IF EXISTS t_bf_left_aj;

CREATE TABLE t_bf_left_aj
(
    tags Array(String),
    INDEX idx_tags tags TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 1;

-- rows 8 and 9 have empty arrays
INSERT INTO t_bf_left_aj SELECT if(number < 8, ['t' || toString(number)], []) FROM numbers(10);

SELECT '-- the filter rejects the default element';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_left_aj LEFT ARRAY JOIN tags AS tag WHERE tag = 't3') WHERE explain LIKE '%Name: idx%' OR explain LIKE '%Granules:%';
SELECT count() FROM t_bf_left_aj LEFT ARRAY JOIN tags AS tag WHERE tag = 't3';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_left_aj LEFT ARRAY JOIN tags AS tag WHERE tag IN ('t3', 't5')) WHERE explain LIKE '%Name: idx%' OR explain LIKE '%Granules:%';
SELECT count() FROM t_bf_left_aj LEFT ARRAY JOIN tags AS tag WHERE tag IN ('t3', 't5');
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_left_aj LEFT ARRAY JOIN tags AS tag WHERE tag GLOBAL IN (SELECT 't3')) WHERE explain LIKE '%Name: idx%' OR explain LIKE '%Granules:%';
SELECT count() FROM t_bf_left_aj LEFT ARRAY JOIN tags AS tag WHERE tag GLOBAL IN (SELECT 't3');

SELECT '-- the filter accepts the default element';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_left_aj LEFT ARRAY JOIN tags AS tag WHERE tag = '') WHERE explain LIKE '%Name: idx%' OR explain LIKE '%Granules:%';
SELECT count() FROM t_bf_left_aj LEFT ARRAY JOIN tags AS tag WHERE tag = '';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_left_aj LEFT ARRAY JOIN tags AS tag WHERE tag IN ('t3', '')) WHERE explain LIKE '%Name: idx%' OR explain LIKE '%Granules:%';
SELECT count() FROM t_bf_left_aj LEFT ARRAY JOIN tags AS tag WHERE tag IN ('t3', '');
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_left_aj LEFT ARRAY JOIN tags AS tag WHERE tag GLOBAL IN (SELECT arrayJoin(['t3', '']))) WHERE explain LIKE '%Name: idx%' OR explain LIKE '%Granules:%';
SELECT count() FROM t_bf_left_aj LEFT ARRAY JOIN tags AS tag WHERE tag GLOBAL IN (SELECT arrayJoin(['t3', '']));

DROP TABLE t_bf_left_aj;

SELECT '-- an index on the padded array keeps the default element';
DROP TABLE IF EXISTS t_bf_padded_aj;
CREATE TABLE t_bf_padded_aj
(
    tags Array(String),
    INDEX idx_padded emptyArrayToSingle(tags) TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 1;
INSERT INTO t_bf_padded_aj SELECT if(number < 8, ['t' || toString(number)], []) FROM numbers(10);
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_padded_aj ARRAY JOIN emptyArrayToSingle(tags) AS tag WHERE tag IN ('t3')) WHERE explain LIKE '%Name: idx%' OR explain LIKE '%Granules:%';
SELECT count() FROM t_bf_padded_aj ARRAY JOIN emptyArrayToSingle(tags) AS tag WHERE tag IN ('t3');
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_padded_aj ARRAY JOIN emptyArrayToSingle(tags) AS tag WHERE tag IN ('t3', '')) WHERE explain LIKE '%Name: idx%' OR explain LIKE '%Granules:%';
SELECT count() FROM t_bf_padded_aj ARRAY JOIN emptyArrayToSingle(tags) AS tag WHERE tag IN ('t3', '');
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_padded_aj LEFT ARRAY JOIN tags AS tag WHERE tag IN ('t3', '')) WHERE explain LIKE '%Name: idx%' OR explain LIKE '%Granules:%';
SELECT count() FROM t_bf_padded_aj LEFT ARRAY JOIN tags AS tag WHERE tag IN ('t3', '');
DROP TABLE t_bf_padded_aj;
