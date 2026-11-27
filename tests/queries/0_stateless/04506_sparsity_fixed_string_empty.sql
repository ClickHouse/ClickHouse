-- Tags: no-old-analyzer
-- no-old-analyzer: Not supported

-- `optimize_empty_string_comparisons = 1` (default) rewrites `s = ''` / `!= ''` into
-- `empty(s)` / `notEmpty(s)` before the sparsity classifier runs. The rewrite must
-- fire for FixedString the same way it fires for String.

SET optimize_trivial_count_query = 1;
SET optimize_trivial_count_with_sparsity_filter = 1;

DROP TABLE IF EXISTS t_fixed_empty;

CREATE TABLE t_fixed_empty (id UInt64, s FixedString(4))
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 512,
         ratio_of_defaults_for_sparse_serialization = 0.5,
         compute_exact_num_defaults_for_sparse_columns = 1,
         serialization_info_version = 'with_types',
         min_bytes_for_wide_part = 0;

INSERT INTO t_fixed_empty
SELECT number, if(number < 3000, '', 'abcd')::FixedString(4)
FROM numbers(5000) SETTINGS optimize_on_insert = 0;

SELECT 'baseline empty:',    countIf(empty(s))    FROM t_fixed_empty;
SELECT 'baseline notEmpty:', countIf(notEmpty(s)) FROM t_fixed_empty;
SELECT 'baseline =empty:',   countIf(s = '')      FROM t_fixed_empty;
SELECT 'baseline !=empty:',  countIf(s != '')     FROM t_fixed_empty;

-- All four spellings must reach the trivial-count-with-sparsity-filter rewrite.
SELECT 'rewrite empty:',    countIf(explain LIKE '%Optimized trivial count with sparsity filter%') FROM (EXPLAIN SELECT count() FROM t_fixed_empty WHERE empty(s));
SELECT 'rewrite notEmpty:', countIf(explain LIKE '%Optimized trivial count with sparsity filter%') FROM (EXPLAIN SELECT count() FROM t_fixed_empty WHERE notEmpty(s));
SELECT 'rewrite =empty:',   countIf(explain LIKE '%Optimized trivial count with sparsity filter%') FROM (EXPLAIN SELECT count() FROM t_fixed_empty WHERE s = '');
SELECT 'rewrite !=empty:',  countIf(explain LIKE '%Optimized trivial count with sparsity filter%') FROM (EXPLAIN SELECT count() FROM t_fixed_empty WHERE s != '');

SELECT 'count empty:',    count() FROM t_fixed_empty WHERE empty(s);
SELECT 'count notEmpty:', count() FROM t_fixed_empty WHERE notEmpty(s);
SELECT 'count =empty:',   count() FROM t_fixed_empty WHERE s = '';
SELECT 'count !=empty:',  count() FROM t_fixed_empty WHERE s != '';

DROP TABLE t_fixed_empty;
