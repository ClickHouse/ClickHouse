-- Tags: no-fasttest
-- no-fasttest: `COLLATE` needs ICU.

-- A top-K read skips granules by the primary index once the threshold tightens. The primary key is in
-- byte order, so a collated threshold must not use it: the granule ('c', 'c', 'c', 'ä') starts beyond
-- the threshold 'b' in the 'en' collation, yet its last row 'ä' collates before 'b' and is the answer.

SET max_threads = 1;
SET max_block_size = 4;
SET optimize_read_in_order = 0;
SET use_top_k_dynamic_filtering = 1;
SET use_top_k_dynamic_filtering_for_variable_length_types = 1;
SET use_skip_indexes_on_data_read = 1;
SET use_query_condition_cache = 0;
SET enable_group_by_top_k_optimization = 1;
SET enable_group_by_top_k_dynamic_filtering = 1;
SET query_plan_max_limit_for_top_k_optimization = 1000;

DROP TABLE IF EXISTS t_top_k_collation;

CREATE TABLE t_top_k_collation (s String) ENGINE = MergeTree ORDER BY s
SETTINGS index_granularity = 4, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_top_k_collation SELECT 'b' FROM numbers(4);
INSERT INTO t_top_k_collation VALUES ('c'), ('c'), ('c'), ('ä');
OPTIMIZE TABLE t_top_k_collation FINAL;

SELECT s FROM t_top_k_collation ORDER BY s COLLATE 'en' LIMIT 1;
SELECT s FROM t_top_k_collation ORDER BY s COLLATE 'en' LIMIT 1 SETTINGS use_top_k_dynamic_filtering = 0;

-- The same through the `GROUP BY` top-K heap.
SELECT s, count() FROM t_top_k_collation GROUP BY s ORDER BY s COLLATE 'en' LIMIT 1;

DROP TABLE t_top_k_collation;
