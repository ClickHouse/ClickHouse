DROP TABLE IF EXISTS t_02147;
DROP TABLE IF EXISTS t_02147_dist;
DROP TABLE IF EXISTS t_02147_merge;

CREATE TABLE t_02147 (date DateTime, v UInt32)
ENGINE = MergeTree ORDER BY toStartOfHour(date);

CREATE TABLE t_02147_dist AS t_02147 ENGINE = Distributed(test_shard_localhost, currentDatabase(), t_02147);
CREATE TABLE t_02147_merge AS t_02147 ENGINE = Merge(currentDatabase(), 't_02147');

SET optimize_monotonous_functions_in_order_by = 1;

EXPLAIN SYNTAX SELECT * FROM t_02147 ORDER BY toStartOfHour(date), v;
EXPLAIN SYNTAX SELECT * FROM t_02147_dist ORDER BY toStartOfHour(date), v;
EXPLAIN SYNTAX SELECT * FROM t_02147_merge ORDER BY toStartOfHour(date), v;
