SET mutations_sync = 2;

DROP TABLE IF EXISTS t_block_number_proj;

CREATE TABLE t_block_number_proj (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a
SETTINGS enable_block_number_column = 1, min_bytes_for_wide_part = 0, index_granularity = 128;

INSERT INTO t_block_number_proj SELECT number, number FROM numbers(1000);

OPTIMIZE TABLE t_block_number_proj FINAL;

ALTER TABLE t_block_number_proj ADD PROJECTION p (SELECT a, b ORDER BY b);
ALTER TABLE t_block_number_proj MATERIALIZE PROJECTION p;

set parallel_replicas_local_plan = 1, parallel_replicas_support_projection = 1, optimize_aggregation_in_order = 0;

SELECT a, b FROM t_block_number_proj WHERE b = 5 SETTINGS force_optimize_projection = 1;

DROP TABLE t_block_number_proj;

DROP TABLE IF EXISTS t_block_number_ttl;

CREATE TABLE t_block_number_ttl (d Date, a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a
SETTINGS enable_block_number_column = 1, min_bytes_for_wide_part = 0, index_granularity = 128;

INSERT INTO t_block_number_ttl VALUES ('2000-10-10', 1, 1) ('2100-10-10', 1, 1);
OPTIMIZE TABLE t_block_number_ttl FINAL;

ALTER TABLE t_block_number_ttl MODIFY TTL d + INTERVAL 1 MONTH;
SELECT * FROM t_block_number_ttl ORDER BY a;

DROP TABLE t_block_number_ttl;
