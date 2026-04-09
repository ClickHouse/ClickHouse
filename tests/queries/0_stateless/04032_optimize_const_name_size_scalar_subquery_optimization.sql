-- Tags: distributed
-- https://github.com/ClickHouse/ClickHouse/issues/98203

-- Scalar doesn't exist with optimize_const_name_size + enable_scalar_subquery_optimization = 0
SELECT ((NULL, 1))
FROM remote('127.0.0.1:9000', view(SELECT 1 AS c0 FROM system.one))
SETTINGS prefer_localhost_replica = 0, optimize_const_name_size = 10, enable_scalar_subquery_optimization = 0;
