-- `rows_before_limit_at_least` remains the number of expanded rows actually seen by the outer
-- query limit. With the optimization enabled only the three selected input rows are expanded, so
-- this lower bound decreases from all 5000 expanded rows to 15. `exact_rows_before_limit` refuses
-- the rewrite and is covered by the EXPLAIN test.

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
