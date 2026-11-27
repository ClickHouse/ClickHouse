-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/12571
-- `CROSS JOIN` with a non-equi filter (e.g. `startsWith`) used to materialize
-- per-input-block output of size `left_block_rows * right_block_rows`, which
-- for wide string columns blew up memory by 20x or more.
-- The output is now bounded by `max_joined_block_size_rows` /
-- `max_joined_block_size_bytes`, so the right side fits in a tight budget.

DROP TABLE IF EXISTS t_cross_starts_with;
CREATE TABLE t_cross_starts_with (id UInt64, name String) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_cross_starts_with SELECT number, repeat('X', 256) FROM numbers(1000);

SET max_memory_usage = '100M';

SELECT count() FROM (
    SELECT q.id, p.id
    FROM t_cross_starts_with AS q, t_cross_starts_with AS p
    WHERE startsWith(q.name, p.name)
);

DROP TABLE t_cross_starts_with;
