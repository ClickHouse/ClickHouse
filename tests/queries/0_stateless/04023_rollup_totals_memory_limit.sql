-- Tags: no-parallel-replicas
-- https://github.com/ClickHouse/ClickHouse/issues/93465
CREATE TABLE t0 (c0 Int, c1 Int128) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t0 SELECT 1, number FROM numbers(40000);
SELECT countDistinct(c1) FROM t0 GROUP BY c0 WITH ROLLUP WITH TOTALS SETTINGS max_memory_usage = 10000000, max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0; -- { serverError MEMORY_LIMIT_EXCEEDED }
