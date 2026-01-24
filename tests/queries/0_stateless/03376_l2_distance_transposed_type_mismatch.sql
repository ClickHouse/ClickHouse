-- Tags: no-parallel-replicas
-- No parallel replicas because: https://github.com/ClickHouse/ClickHouse/issues/74367

-- https://github.com/ClickHouse/ClickHouse/issues/89976
-- Check whether the distance is calculated correctly when query_vec has a different type than qbit

SET allow_experimental_qbit_type = 1;

DROP TABLE IF EXISTS test;

CREATE TABLE test 
(
    id UInt32, 
    qbit QBit(Float32, 16)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO test VALUES (1, [-0.042587746,0.029204812,-0.018542241,-0.0006326993,-0.046840265,0.017869968,-0.036177695,0.008778641,-0.0062302556,0.030549359,-0.009787052,-0.01996496,-0.0034493103,-0.01415683,-0.04583967,-0.047684517]);

SELECT 'Test L2DistanceTransposed(qbit, ref_vec, precision)';
-------------------------------------------------------------
WITH [-0.042587746, 0.029204812, -0.018542241, -0.0006326993, -0.046840265, 0.017869968, -0.036177695, 0.008778641, -0.0062302556, 0.030549359, -0.009787052, -0.01996496, -0.0034493103, -0.01415683, -0.04583967, -0.047684517] AS query_vec
SELECT id, L2DistanceTransposed(qbit, query_vec, 32) AS distance
FROM test SETTINGS enable_analyzer=1;

WITH [-0.042587746, 0.029204812, -0.018542241, -0.0006326993, -0.046840265, 0.017869968, -0.036177695, 0.008778641, -0.0062302556, 0.030549359, -0.009787052, -0.01996496, -0.0034493103, -0.01415683, -0.04583967, -0.047684517] AS query_vec
SELECT id, L2DistanceTransposed(qbit, query_vec, 32) AS distance
FROM test SETTINGS enable_analyzer=0;


SELECT 'Test L2DistanceTransposed(qbit.1, ..., qbit.precision, qbit_dim, ref_vec)';
-----------------------------------------------------------------------------------
WITH [-0.042587746, 0.029204812, -0.018542241, -0.0006326993, -0.046840265, 0.017869968, -0.036177695, 0.008778641, -0.0062302556, 0.030549359, -0.009787052, -0.01996496, -0.0034493103, -0.01415683, -0.04583967, -0.047684517] AS query_vec
SELECT
    id,
    L2DistanceTransposed(qbit.1, qbit.2, qbit.3, qbit.4, qbit.5, qbit.6, qbit.7, qbit.8, qbit.9, qbit.10, qbit.11, qbit.12, qbit.13, qbit.14, qbit.15, qbit.16, qbit.17, qbit.18, qbit.19, qbit.20, qbit.21, qbit.22, qbit.23, qbit.24, qbit.25, qbit.26, qbit.27, qbit.28, qbit.29, qbit.30, qbit.31, qbit.32,
                         16,
                         CAST(query_vec, 'Array(Float32)')
                        ) AS distance
FROM test;

-- This should expectedly give a wrong distance because the type of query_vec doesn't match the type of qbit
WITH [-0.042587746, 0.029204812, -0.018542241, -0.0006326993, -0.046840265, 0.017869968, -0.036177695, 0.008778641, -0.0062302556, 0.030549359, -0.009787052, -0.01996496, -0.0034493103, -0.01415683, -0.04583967, -0.047684517] AS query_vec
SELECT
    id,
    L2DistanceTransposed(qbit.1, qbit.2, qbit.3, qbit.4, qbit.5, qbit.6, qbit.7, qbit.8, qbit.9, qbit.10, qbit.11, qbit.12, qbit.13, qbit.14, qbit.15, qbit.16, qbit.17, qbit.18, qbit.19, qbit.20, qbit.21, qbit.22, qbit.23, qbit.24, qbit.25, qbit.26, qbit.27, qbit.28, qbit.29, qbit.30, qbit.31, qbit.32,
                         16,
                         CAST(query_vec, 'Array(Float64)')
                        ) AS distance
FROM test;

-- Test with more columns than the function expects
WITH [-0.042587746, 0.029204812, -0.018542241, -0.0006326993, -0.046840265, 0.017869968, -0.036177695, 0.008778641, -0.0062302556, 0.030549359, -0.009787052, -0.01996496, -0.0034493103, -0.01415683, -0.04583967, -0.047684517] AS query_vec
SELECT
    id,
    L2DistanceTransposed(qbit.1, qbit.2, qbit.3, qbit.4, qbit.5, qbit.6, qbit.7, qbit.8, qbit.9, qbit.10, qbit.11, qbit.12, qbit.13, qbit.14, qbit.15, qbit.16, qbit.17, qbit.18, qbit.19, qbit.20, qbit.21, qbit.22, qbit.23, qbit.24, qbit.25, qbit.26, qbit.27, qbit.28, qbit.29, qbit.30, qbit.31, qbit.32, qbit.32,
                         16,
                         CAST(query_vec, 'Array(Float32)')
                        ) AS distance
FROM test; -- { serverError TOO_MANY_ARGUMENTS_FOR_FUNCTION }


DROP TABLE test;
