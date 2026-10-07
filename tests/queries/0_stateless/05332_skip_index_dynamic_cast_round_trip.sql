-- skip indexes work through x::Dynamic::T when x converts to T losslessly

SET explain_query_plan_default = 'legacy';
SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 0;
SET use_query_condition_cache = 0;

DROP TABLE IF EXISTS t_dynamic_round_trip;

CREATE TABLE t_dynamic_round_trip
(
    n UInt64,
    s String,
    doc JSON(name String),
    INDEX idx_n n TYPE bloom_filter GRANULARITY 1,
    INDEX idx_s s TYPE bloom_filter GRANULARITY 1,
    INDEX idx_doc_name doc.name TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 1;

INSERT INTO t_dynamic_round_trip SELECT
    number,
    's' || toString(number),
    toJSONString(map('name', 'n' || toString(number)))::JSON
FROM numbers(10);

SELECT '-- same type';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_dynamic_round_trip WHERE s::Dynamic::String = 's3') WHERE explain LIKE '%Name: idx%' OR explain LIKE '%Granules:%';
SELECT count() FROM t_dynamic_round_trip WHERE s::Dynamic::String = 's3';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_dynamic_round_trip WHERE doc.name::Dynamic::String IN ('n3', 'n5')) WHERE explain LIKE '%Name: idx%' OR explain LIKE '%Granules:%';
SELECT count() FROM t_dynamic_round_trip WHERE doc.name::Dynamic::String IN ('n3', 'n5');

SELECT '-- Nullable added';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_dynamic_round_trip WHERE s::Dynamic::Nullable(String) = 's3') WHERE explain LIKE '%Name: idx%' OR explain LIKE '%Granules:%';
SELECT count() FROM t_dynamic_round_trip WHERE s::Dynamic::Nullable(String) = 's3';

SELECT '-- the conversion changes the value';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_dynamic_round_trip WHERE n::Dynamic::String = '3') WHERE explain LIKE '%Name: idx%' OR explain LIKE '%Granules:%';
SELECT count() FROM t_dynamic_round_trip WHERE n::Dynamic::String = '3';

DROP TABLE t_dynamic_round_trip;
