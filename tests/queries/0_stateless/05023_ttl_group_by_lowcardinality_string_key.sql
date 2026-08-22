DROP TABLE IF EXISTS t_ttl_group_by_lc_string;

CREATE TABLE t_ttl_group_by_lc_string (k LowCardinality(String), d DateTime, v UInt64)
ENGINE = MergeTree ORDER BY k
TTL d + INTERVAL 1 SECOND GROUP BY k SET v = sum(v), d = max(d)
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 1024, merge_with_ttl_timeout = 100000,
         merge_max_block_size = 8192;

-- One block holding many distinct keys in no particular order, so the dictionary is still
-- growing while later key runs of the same block are aggregated.
INSERT INTO t_ttl_group_by_lc_string
SELECT number % 1000, now() - INTERVAL 1 DAY, number FROM numbers(200000)
SETTINGS max_insert_block_size = 200000;

OPTIMIZE TABLE t_ttl_group_by_lc_string FINAL;

SELECT count(), sum(v) FROM t_ttl_group_by_lc_string;

SELECT k, v FROM t_ttl_group_by_lc_string ORDER BY k LIMIT 3;

DROP TABLE t_ttl_group_by_lc_string;
