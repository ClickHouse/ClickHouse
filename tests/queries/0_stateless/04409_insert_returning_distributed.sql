-- INSERT ... SELECT ... RETURNING into a Distributed target with parallel_distributed_insert_select
-- enabled must run RETURNING once on the initiator after the insert phase, not push the RETURNING
-- subquery to each shard. Regression for the distributed insert-select optimizations serializing the
-- returning_select into the remote INSERT.

SET async_insert = 0;
SET parallel_distributed_insert_select = 1;
SET distributed_foreground_insert = 1;

DROP TABLE IF EXISTS t_ret_dist_local;
DROP TABLE IF EXISTS t_ret_dist;
DROP TABLE IF EXISTS t_ret_dist_src;

CREATE TABLE t_ret_dist_local (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_ret_dist AS t_ret_dist_local ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), t_ret_dist_local, id);
CREATE TABLE t_ret_dist_src (id UInt64) ENGINE = Memory;
INSERT INTO t_ret_dist_src VALUES (1), (2), (3);

-- RETURNING runs once on the initiator and returns a single row (a literal keeps the result independent
-- of the two-shards-over-one-local-table topology). With the bug the subquery would be pushed to each
-- shard and the streamed result would break the remote insert.
SELECT 'distributed insert select returning';
INSERT INTO t_ret_dist SELECT id FROM t_ret_dist_src
RETURNING (SELECT 'returned-on-initiator');

-- The rows were actually inserted into the underlying local table.
SELECT 'rows in local table';
SELECT id FROM t_ret_dist_local ORDER BY id;

DROP TABLE t_ret_dist_src;
DROP TABLE t_ret_dist;
DROP TABLE t_ret_dist_local;
