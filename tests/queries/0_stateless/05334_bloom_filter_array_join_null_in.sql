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
    doc JSON(tags Array(String)),
    INDEX idx_tags tags TYPE bloom_filter GRANULARITY 1,
    INDEX idx_doc_tags doc.tags TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 1;

-- rows 8 and 9 have empty arrays
INSERT INTO t_bf_aj_null_in SELECT
    if(number < 8, ['t' || toString(number)], []),
    toJSONString(map('tags', if(number < 8, ['t' || toString(number)], [])))::JSON
FROM numbers(10);

SELECT '-- set without NULL';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_aj_null_in WHERE arrayJoin(tags) IN ('t3')) WHERE explain LIKE '%Name: idx%' OR explain LIKE '%Granules:%';
SELECT count() FROM t_bf_aj_null_in WHERE arrayJoin(tags) IN ('t3');
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_aj_null_in WHERE arrayJoin(tags) GLOBAL IN (SELECT 't3')) WHERE explain LIKE '%Name: idx%' OR explain LIKE '%Granules:%';
SELECT count() FROM t_bf_aj_null_in WHERE arrayJoin(tags) GLOBAL IN (SELECT 't3');

SELECT '-- set with NULL, empty arrays become NULL';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_aj_null_in WHERE arrayJoin(emptyArrayToSingle(tags::Array(Nullable(String)))) IN ('t3', NULL)) WHERE explain LIKE '%Name: idx%' OR explain LIKE '%Granules:%';
SELECT count() FROM t_bf_aj_null_in WHERE arrayJoin(emptyArrayToSingle(tags::Array(Nullable(String)))) IN ('t3', NULL);

SELECT '-- emptyArrayToSingle, Dynamic round trip and Nullable result on a JSON path';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_aj_null_in WHERE arrayJoin(emptyArrayToSingle(doc.tags::Dynamic::Array(String)))::Nullable(String) GLOBAL IN (SELECT 't3'::Nullable(String))) WHERE explain LIKE '%Name: idx%' OR explain LIKE '%Granules:%';
SELECT count() FROM t_bf_aj_null_in WHERE arrayJoin(emptyArrayToSingle(doc.tags::Dynamic::Array(String)))::Nullable(String) GLOBAL IN (SELECT 't3'::Nullable(String));

SELECT '-- the same, arrayJoin stays a function';
SET query_plan_lower_array_join_function = 0;
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_aj_null_in WHERE arrayJoin(emptyArrayToSingle(doc.tags::Dynamic::Array(String)))::Nullable(String) GLOBAL IN (SELECT 't3'::Nullable(String))) WHERE explain LIKE '%Name: idx%' OR explain LIKE '%Granules:%';
SELECT count() FROM t_bf_aj_null_in WHERE arrayJoin(emptyArrayToSingle(doc.tags::Dynamic::Array(String)))::Nullable(String) GLOBAL IN (SELECT 't3'::Nullable(String));

DROP TABLE t_bf_aj_null_in;
