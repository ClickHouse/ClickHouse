SET enable_analyzer = 1, enable_early_constant_folding = 1, enable_parallel_replicas = 0;
SET use_skip_indexes = 1, use_skip_indexes_on_data_read = 0, use_query_condition_cache = 0;

CREATE TABLE nullable_empty_prewhere (id UInt64, n Nullable(UInt64), INDEX bf id TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO nullable_empty_prewhere VALUES (1, NULL), (2, 2);

SELECT count() FROM viewExplain('EXPLAIN', 'indexes = 1',
    (SELECT id FROM nullable_empty_prewhere PREWHERE n IN (SELECT toUInt64(1) WHERE false) WHERE id = 1))
WHERE explain LIKE '%Name: bf%';

SELECT count() FROM viewExplain('EXPLAIN', 'indexes = 1',
    (SELECT id FROM nullable_empty_prewhere PREWHERE n GLOBAL IN (SELECT toUInt64(1) WHERE false) WHERE id = 1))
WHERE explain LIKE '%Name: bf%';

SELECT count() FROM viewExplain('EXPLAIN', 'indexes = 1',
    (SELECT id FROM nullable_empty_prewhere PREWHERE id > 0 AND n IN (SELECT toUInt64(1) WHERE false) WHERE id = 1))
WHERE explain LIKE '%Name: bf%';

SELECT count() FROM nullable_empty_prewhere PREWHERE n IN (SELECT toUInt64(1) WHERE false);
SELECT count() FROM nullable_empty_prewhere PREWHERE n IN (SELECT toUInt64(2));
DROP TABLE nullable_empty_prewhere;
