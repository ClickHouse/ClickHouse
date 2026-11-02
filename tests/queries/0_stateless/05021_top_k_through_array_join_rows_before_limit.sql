-- Tags: no-parallel-replicas
-- no-parallel-replicas: the counted rows depend on the local pipeline

-- `rows_before_limit_at_least` counts the expanded rows the limit sees: all 5000, or 15 when only the top 3 rows are expanded

DROP TABLE IF EXISTS t_aj_rbl;

CREATE TABLE t_aj_rbl (x UInt64, arr Array(UInt32))
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_aj_rbl SELECT number, range(1, 6) FROM numbers(1000);

SET output_format_write_statistics = 0;
SET format_template_row_format = '${0:Raw}';
SET format_template_rows_between_delimiter = '';
SET format_template_resultset_format = '${data}{"rows_before_limit_at_least":${rows_before_limit:Raw}}\n';
SET query_plan_max_limit_for_top_k_optimization = 0;
SET max_block_size = 100;

SELECT '-- ARRAY JOIN, query_plan_top_k_through_array_join = 0';
SELECT '' FROM t_aj_rbl ARRAY JOIN arr ORDER BY x LIMIT 3
SETTINGS query_plan_top_k_through_array_join = 0
FORMAT Template;

SELECT '-- LEFT ARRAY JOIN, query_plan_top_k_through_array_join = 0';
SELECT '' FROM t_aj_rbl LEFT ARRAY JOIN arr ORDER BY x LIMIT 3
SETTINGS query_plan_top_k_through_array_join = 0
FORMAT Template;

SELECT '-- ARRAY JOIN, query_plan_top_k_through_array_join = 1';
SELECT '' FROM t_aj_rbl ARRAY JOIN arr ORDER BY x LIMIT 3
SETTINGS query_plan_top_k_through_array_join = 1
FORMAT Template;

SELECT '-- LEFT ARRAY JOIN, query_plan_top_k_through_array_join = 1';
SELECT '' FROM t_aj_rbl LEFT ARRAY JOIN arr ORDER BY x LIMIT 3
SETTINGS query_plan_top_k_through_array_join = 1
FORMAT Template;

DROP TABLE t_aj_rbl;
