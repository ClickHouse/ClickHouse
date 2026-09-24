-- The top-K granule skipping through the primary index reads granule bounds assuming the key column
-- ascends within a part. With a descending key column (`allow_experimental_reverse_key`) that is wrong,
-- so the column must be left out of the pruning; otherwise `ORDER BY A DESC LIMIT` skips granules that
-- hold the top rows.

SET serialize_query_plan = 0;
SET enable_parallel_replicas = 0;
SET optimize_read_in_order = 0;
SET use_top_k_dynamic_filtering = 1;
SET use_skip_indexes_on_data_read = 1;
SET query_plan_max_limit_for_top_k_optimization = 1000;
SET max_threads = 1;
SET max_block_size = 8192;

DROP TABLE IF EXISTS t_top_k_reverse_key;

CREATE TABLE t_top_k_reverse_key (A Int64) ENGINE = MergeTree
PARTITION BY (A % 64) ORDER BY A DESC
SETTINGS allow_experimental_reverse_key = 1, index_granularity = 8192;

INSERT INTO t_top_k_reverse_key SELECT intDiv(number, 11111) FROM numbers(7e5) UNION ALL SELECT number FROM numbers(7e5);

SELECT groupArray(A) FROM (SELECT A FROM t_top_k_reverse_key ORDER BY A DESC LIMIT 10);
SELECT groupArray(A) FROM (SELECT A FROM t_top_k_reverse_key ORDER BY A LIMIT 10);

DROP TABLE t_top_k_reverse_key;
