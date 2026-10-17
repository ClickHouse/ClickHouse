-- Test for issue https://github.com/ClickHouse/ClickHouse/issues/97792

SET parallel_replicas_local_plan = 1;

DROP TABLE IF EXISTS t_map_tokenbf;

CREATE TABLE t_map_tokenbf
(
    metadata Map(String, String),
    created_at DateTime64(3),
    INDEX index_metadata_keys mapKeys(metadata) TYPE tokenbf_v1(32768, 3, 0) GRANULARITY 1,
    INDEX index_metadata_vals mapValues(metadata) TYPE tokenbf_v1(32768, 3, 0) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY created_at;

INSERT INTO t_map_tokenbf VALUES ({'hostname': 'myhost', 'env': 'prod'}, now());

SELECT count() FROM t_map_tokenbf WHERE mapContainsKeyLike(metadata, '%host%'); -- 1
SELECT count() FROM t_map_tokenbf WHERE mapContainsKeyLike(metadata, '%bad%'); -- 0
SELECT count() FROM t_map_tokenbf WHERE mapContains(metadata, 'hostname'); -- 1
SELECT count() FROM t_map_tokenbf WHERE mapContainsKey(metadata, 'env'); -- 1
SELECT count() FROM t_map_tokenbf WHERE has(mapKeys(metadata), 'env'); -- 1
SELECT count() FROM t_map_tokenbf WHERE has(metadata, 'hostname'); -- 1
SELECT count() FROM t_map_tokenbf WHERE mapContainsValue(metadata, 'prod'); -- 1
SELECT count() FROM t_map_tokenbf WHERE mapContainsValueLike(metadata, '%host%'); -- 1
SELECT count() FROM t_map_tokenbf WHERE mapContainsValueLike(metadata, '%random%'); -- 0

SELECT arrayJoin(mapKeys(mapExtractKeyLike(metadata, '%host%'))) as extracted_metadata
FROM t_map_tokenbf
WHERE mapContainsKeyLike(metadata, '%host%')
GROUP BY extracted_metadata;

-- Verify that skip index was used - all should return 1
SELECT 'Verify skip index is used';

SELECT COUNT(*) FROM (
        EXPLAIN indexes=1 SELECT count() FROM t_map_tokenbf WHERE mapContainsKeyLike(metadata, '%host%')
    ) WHERE explain LIKE '%index_metadata%';

SELECT COUNT(*) FROM (
        EXPLAIN indexes=1 SELECT count() FROM t_map_tokenbf WHERE mapContains(metadata, 'hostname')
    ) WHERE explain LIKE '%index_metadata%';

SELECT COUNT(*) FROM (
        EXPLAIN indexes=1 SELECT count() FROM t_map_tokenbf WHERE mapContainsKey(metadata, 'env')
    ) WHERE explain LIKE '%index_metadata%';

SELECT COUNT(*) FROM (
        EXPLAIN indexes=1 SELECT count() FROM t_map_tokenbf WHERE has(mapKeys(metadata), 'env')
    ) WHERE explain LIKE '%index_metadata%';

SELECT COUNT(*) FROM (
        EXPLAIN indexes=1 SELECT count() FROM t_map_tokenbf WHERE has(metadata, 'hostname') 
    ) WHERE explain LIKE '%index_metadata%';

SELECT COUNT(*) FROM (
        EXPLAIN indexes=1 SELECT count() FROM t_map_tokenbf WHERE mapContainsValue(metadata, 'prod')
    ) WHERE explain LIKE '%index_metadata%';

SELECT COUNT(*) FROM (
        EXPLAIN indexes=1 SELECT count() FROM t_map_tokenbf WHERE mapContainsValueLike(metadata, '%random%')
    ) WHERE explain LIKE '%index_metadata%';

DROP TABLE t_map_tokenbf;
