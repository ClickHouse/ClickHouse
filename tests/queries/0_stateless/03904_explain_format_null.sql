-- Test that FORMAT Null is ignored for EXPLAIN queries when setting is enabled (default)

SET enable_analyzer = 1, max_threads = 2;

EXPLAIN SELECT number FROM numbers_mt(1000) GROUP BY number FORMAT Null;

SELECT '------------------' as separator;

EXPLAIN SYNTAX SELECT number FROM numbers_mt(1000) GROUP BY number FORMAT Null;

SELECT '------------------' as separator;

EXPLAIN PLAN SELECT number FROM numbers_mt(1000) GROUP BY number FORMAT Null;

SELECT '------------------' as separator;

EXPLAIN PIPELINE SELECT number FROM numbers_mt(1000) GROUP BY number FORMAT Null;

-- Test backward compatible behavior with setting disabled
SET ignore_format_null_for_explain = 0;

EXPLAIN SELECT number FROM numbers_mt(1000) GROUP BY number FORMAT Null;

EXPLAIN SYNTAX SELECT number FROM numbers_mt(1000) GROUP BY number FORMAT Null;

EXPLAIN PLAN SELECT number FROM numbers_mt(1000) GROUP BY number FORMAT Null;

EXPLAIN PIPELINE SELECT number FROM numbers_mt(1000) GROUP BY number FORMAT Null;
