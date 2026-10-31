SET enable_analyzer = 1, enable_parallel_replicas = 0;
SET use_skip_indexes = 1, use_skip_indexes_on_data_read = 0, use_query_condition_cache = 0;

CREATE TABLE mutable_prewhere_set (n UInt64) ENGINE = Set;
CREATE TABLE mutable_prewhere_source (id UInt64, n Nullable(UInt64), INDEX bf id TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO mutable_prewhere_source VALUES (1, 1), (2, 2), (3, NULL);

SELECT count() FROM viewExplain('EXPLAIN', 'indexes = 1',
    (SELECT id FROM mutable_prewhere_source PREWHERE n IN mutable_prewhere_set WHERE id = 1))
WHERE explain LIKE '%Name: bf%';

SELECT count() FROM mutable_prewhere_source PREWHERE n IN mutable_prewhere_set;
INSERT INTO mutable_prewhere_set VALUES (1);
SELECT id FROM mutable_prewhere_source PREWHERE n IN mutable_prewhere_set;

DROP TABLE mutable_prewhere_source;
DROP TABLE mutable_prewhere_set;
