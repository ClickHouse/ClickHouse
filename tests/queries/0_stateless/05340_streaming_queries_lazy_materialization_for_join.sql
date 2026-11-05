-- Tags: no-parallel-replicas
-- Lazy materialization over joins does not defer the columns of a STREAM read: such a read selects its parts and
-- ranges during execution, so a lazy read could not address the same rows. The other side of the join is still read
-- lazily. Bounded streaming reads everything committed so far and finishes, so it runs synchronously.

SET enable_streaming_queries = 1;
SET query_plan_optimize_lazy_materialization = 1;
SET query_plan_lazy_materialization_for_join = 1;
SET query_plan_max_limit_for_lazy_materialization = 10000;
SET max_threads = 1;
-- Pinned so that the join keeps the order it is written in.
SET enable_join_runtime_filters = 0;
SET query_plan_join_swap_table = 0;
SET query_plan_optimize_join_order_limit = 0;

DROP TABLE IF EXISTS t_stream;
DROP TABLE IF EXISTS t_dim;

CREATE TABLE t_stream (id UInt64, region String, value UInt64) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 16, enable_block_number_column = 1, enable_block_offset_column = 1;
CREATE TABLE t_dim (id UInt64, payload String) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 16;

INSERT INTO t_stream SELECT number, if(number BETWEEN 1600 AND 1610, 'rare', 'common'), number * 10 FROM numbers(4096);
INSERT INTO t_dim SELECT number, concat('p', toString(number)) FROM numbers(4096);

SELECT s.id, s.value, d.payload FROM t_stream AS s STREAM BOUNDED JOIN t_dim AS d ON s.id = d.id
WHERE s.region = 'rare' ORDER BY s.id LIMIT 5;

-- Only `payload` of the other table is read lazily.
SELECT trim(explain) FROM (EXPLAIN
    SELECT s.id, s.value, d.payload FROM t_stream AS s STREAM BOUNDED JOIN t_dim AS d ON s.id = d.id
    WHERE s.region = 'rare' ORDER BY s.id LIMIT 5)
WHERE explain LIKE '%Lazily read columns%';

DROP TABLE t_stream;
DROP TABLE t_dim;
