-- Tags: no-fasttest, no-parallel

SET enable_analyzer = 1;

-- Test LEFT JOIN
SELECT 'LEFT JOIN';
SELECT * 
FROM s3Cluster(test_shard_localhost, 'http://localhost:11111/test/03800_a.parquet', 'NOSIGN', 'Parquet', 'boolean_col Boolean, long_col Int64') AS t1 
LEFT JOIN s3Cluster(test_shard_localhost, 'http://localhost:11111/test/03800_b.parquet', 'NOSIGN', 'Parquet', 'boolean_col Boolean, long_col Int64') AS t2 
ON t1.boolean_col = t2.boolean_col 
ORDER BY t1.boolean_col;

-- Test INNER JOIN
SELECT 'INNER JOIN';
SELECT * 
FROM s3Cluster(test_shard_localhost, 'http://localhost:11111/test/03800_a.parquet', 'NOSIGN', 'Parquet', 'boolean_col Boolean, long_col Int64') AS t1 
INNER JOIN s3Cluster(test_shard_localhost, 'http://localhost:11111/test/03800_b.parquet', 'NOSIGN', 'Parquet', 'boolean_col Boolean, long_col Int64') AS t2 
ON t1.boolean_col = t2.boolean_col 
ORDER BY t1.boolean_col;

-- Test RIGHT JOIN
SELECT 'RIGHT JOIN';
SELECT * 
FROM s3Cluster(test_shard_localhost, 'http://localhost:11111/test/03800_a.parquet', 'NOSIGN', 'Parquet', 'boolean_col Boolean, long_col Int64') AS t1 
RIGHT JOIN s3Cluster(test_shard_localhost, 'http://localhost:11111/test/03800_b.parquet', 'NOSIGN', 'Parquet', 'boolean_col Boolean, long_col Int64') AS t2 
ON t1.boolean_col = t2.boolean_col 
ORDER BY t2.boolean_col;

-- Test LEFT OUTER JOIN
SELECT 'LEFT OUTER JOIN';
SELECT * 
FROM s3Cluster(test_shard_localhost, 'http://localhost:11111/test/03800_a.parquet', 'NOSIGN', 'Parquet', 'boolean_col Boolean, long_col Int64') AS t1 
LEFT OUTER JOIN s3Cluster(test_shard_localhost, 'http://localhost:11111/test/03800_b.parquet', 'NOSIGN', 'Parquet', 'boolean_col Boolean, long_col Int64') AS t2 
ON t1.boolean_col = t2.boolean_col 
ORDER BY t1.boolean_col;

-- Test FULL OUTER JOIN
SELECT 'FULL OUTER JOIN';
SELECT * 
FROM s3Cluster(test_shard_localhost, 'http://localhost:11111/test/03800_a.parquet', 'NOSIGN', 'Parquet', 'boolean_col Boolean, long_col Int64') AS t1 
FULL OUTER JOIN s3Cluster(test_shard_localhost, 'http://localhost:11111/test/03800_b.parquet', 'NOSIGN', 'Parquet', 'boolean_col Boolean, long_col Int64') AS t2 
ON t1.boolean_col = t2.boolean_col 
ORDER BY t1.boolean_col, t2.boolean_col;

-- Test that simple SELECT still works (no JOIN)
SELECT 'Simple SELECT';
SELECT * 
FROM s3Cluster(test_shard_localhost, 'http://localhost:11111/test/03800_a.parquet', 'NOSIGN', 'Parquet', 'boolean_col Boolean, long_col Int64') 
ORDER BY boolean_col;
