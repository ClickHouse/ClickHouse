-- Tags: no-replicated-database
-- Tag no-replicated-database: ON CLUSTER is not allowed for Replicated databases

-- `ON CLUSTER` can be written without a cluster name; the name is taken from the `default_cluster` setting.

-- Suppress the per-host status table so the output does not depend on the host name.
SET distributed_ddl_output_mode = 'none';

SET default_cluster = 'test_shard_localhost';

DROP TABLE IF EXISTS t_on_cluster_default ON CLUSTER;
CREATE TABLE t_on_cluster_default ON CLUSTER (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_on_cluster_default VALUES (1), (2), (3);
SELECT sum(x) FROM t_on_cluster_default;
DROP TABLE t_on_cluster_default ON CLUSTER;

-- When `default_cluster` refers to a cluster that does not exist, resolving the empty `ON CLUSTER` clause fails.
SET default_cluster = 'cluster_that_does_not_exist_04505';
CREATE TABLE t_on_cluster_default ON CLUSTER (x UInt64) ENGINE = MergeTree ORDER BY x; -- { serverError CLUSTER_DOESNT_EXIST }
