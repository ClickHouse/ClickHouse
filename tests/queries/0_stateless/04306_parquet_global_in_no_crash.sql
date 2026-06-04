-- Tags: no-fasttest, no-random-settings
-- Tag no-fasttest: depends on MinIO via s3_conn.
-- Tag no-random-settings: depends on Parquet push-down defaults.
-- GLOBAL IN on Parquet must not eagerly build the set (it would crash).
-- buildSetsForDAGExcludingGlobalIn skips globalIn sets.

INSERT INTO FUNCTION s3(s3_conn,
    filename = '04306_parquet_global_in/' || currentDatabase() || '.parquet',
    format = Parquet,
    structure = 'id UInt64')
SELECT number FROM numbers(1000);

-- Functional test: GLOBAL IN via s3Cluster must return correct results.
SELECT count()
FROM s3Cluster('test_cluster_two_shards', s3_conn,
    filename = '04306_parquet_global_in/' || currentDatabase() || '.parquet',
    format = Parquet, structure = 'id UInt64')
WHERE id GLOBAL IN (SELECT arrayJoin([1, 500, 999])::UInt64);

-- EXPLAIN: CreatingSet SHOULD appear (global set not built eagerly).
SELECT 'global_in_lazy',
    countIf(explain LIKE '%CreatingSet%') > 0 AS lazy
FROM (EXPLAIN
    SELECT count()
    FROM s3Cluster('test_cluster_two_shards', s3_conn,
        filename = '04306_parquet_global_in/' || currentDatabase() || '.parquet',
        format = Parquet, structure = 'id UInt64')
    WHERE id GLOBAL IN (SELECT arrayJoin([1])::UInt64));
