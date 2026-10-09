-- LEFT ARRAY JOIN and arrayJoin(emptyArrayToSingle(x)) use the bloom filter unless the filter accepts the default

SET explain_query_plan_default = 'legacy';
SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 0;
SET use_query_condition_cache = 0;

DROP TABLE IF EXISTS t_bf_aj;

CREATE TABLE t_bf_aj
(
    tags Array(String),
    doc JSON(tags Array(String)),
    INDEX idx_tags tags TYPE bloom_filter GRANULARITY 1,
    INDEX idx_doc_tags doc.tags TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 1;

-- rows 8 and 9 have empty arrays
INSERT INTO t_bf_aj SELECT
    if(number < 8, ['t' || toString(number)], []),
    toJSONString(map('tags', if(number < 8, ['t' || toString(number)], [])))::JSON
FROM numbers(10);

SELECT '-- emptyArrayToSingle';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_aj WHERE arrayJoin(emptyArrayToSingle(tags)) = 't3') WHERE explain LIKE '%Name: idx%' OR explain LIKE '%Granules:%';
SELECT count() FROM t_bf_aj WHERE arrayJoin(emptyArrayToSingle(tags)) = 't3';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_aj WHERE arrayJoin(emptyArrayToSingle(tags)) IN ('t3', 't5')) WHERE explain LIKE '%Name: idx%' OR explain LIKE '%Granules:%';
SELECT count() FROM t_bf_aj WHERE arrayJoin(emptyArrayToSingle(tags)) IN ('t3', 't5');

SELECT '-- emptyArrayToSingle, the filter accepts the default element';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_aj WHERE arrayJoin(emptyArrayToSingle(tags)) IN ('t3', '')) WHERE explain LIKE '%Name: idx%' OR explain LIKE '%Granules:%';
SELECT count() FROM t_bf_aj WHERE arrayJoin(emptyArrayToSingle(tags)) IN ('t3', '');

SELECT '-- LEFT ARRAY JOIN';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_aj LEFT ARRAY JOIN tags AS tag WHERE tag = 't3') WHERE explain LIKE '%Name: idx%' OR explain LIKE '%Granules:%';
SELECT count() FROM t_bf_aj LEFT ARRAY JOIN tags AS tag WHERE tag = 't3';

SELECT '-- LEFT ARRAY JOIN, the filter accepts the default element';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_aj LEFT ARRAY JOIN tags AS tag WHERE tag = '') WHERE explain LIKE '%Name: idx%' OR explain LIKE '%Granules:%';
SELECT count() FROM t_bf_aj LEFT ARRAY JOIN tags AS tag WHERE tag = '';

SELECT '-- subquery set';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_aj WHERE arrayJoin(emptyArrayToSingle(tags)) GLOBAL IN (SELECT 't3')) WHERE explain LIKE '%Name: idx%' OR explain LIKE '%Granules:%';
SELECT count() FROM t_bf_aj WHERE arrayJoin(emptyArrayToSingle(tags)) GLOBAL IN (SELECT 't3');
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_aj WHERE arrayJoin(emptyArrayToSingle(tags)) GLOBAL IN (SELECT arrayJoin(['t3', '']))) WHERE explain LIKE '%Name: idx%' OR explain LIKE '%Granules:%';
SELECT count() FROM t_bf_aj WHERE arrayJoin(emptyArrayToSingle(tags)) GLOBAL IN (SELECT arrayJoin(['t3', '']));

SELECT '-- Dynamic round trip on a JSON path';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_aj WHERE arrayJoin(emptyArrayToSingle(doc.tags::Dynamic::Array(String))) = 't3') WHERE explain LIKE '%Name: idx%' OR explain LIKE '%Granules:%';
SELECT count() FROM t_bf_aj WHERE arrayJoin(emptyArrayToSingle(doc.tags::Dynamic::Array(String))) = 't3';

SELECT '-- arrayJoin stays a function';
SET query_plan_lower_array_join_function = 0;
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_aj WHERE arrayJoin(emptyArrayToSingle(tags)) = 't3') WHERE explain LIKE '%Name: idx%' OR explain LIKE '%Granules:%';
SELECT count() FROM t_bf_aj WHERE arrayJoin(emptyArrayToSingle(tags)) = 't3';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_aj WHERE arrayJoin(emptyArrayToSingle(tags)) IN ('t3', '')) WHERE explain LIKE '%Name: idx%' OR explain LIKE '%Granules:%';
SELECT count() FROM t_bf_aj WHERE arrayJoin(emptyArrayToSingle(tags)) IN ('t3', '');

DROP TABLE t_bf_aj;
