-- Binary arithmetic functions (plus/minus/multiply/...) resolve their interval/tuple sub-function
-- builders at build time and cache the Date/Time overflow behavior, so executeImpl never consults
-- the query context. This keeps a stored expression (e.g. a sorting key) executable during a merge
-- after the context that built it has been destroyed. Related: issue #54890.
--
-- Scope note: the tuple special cases (tuple + tuple, Date + tuple of intervals, interval + interval,
-- tuple * number) are exercised below for behavior, but their cached sub-functions still hold a strong
-- context reference internally, so those paths still retain (and are kept alive by) the build-time
-- context. The same applies to arrays whose element-level operation is one of those cases (arrays of
-- tuples, arrays of intervals): the element-level sibling function caches the same tuple-family
-- sub-functions. Only the plain numeric/Date/interval paths, and arrays over them, are fully
-- context-free. Making the tuple function family context-free is a planned follow-up.

SET session_timezone = 'UTC';

-- Interval arithmetic must resolve the same sub-function for plain, Nullable and LowCardinality
-- operands (the argument types are normalized the same way executeImpl sees them).
SELECT toDate('2020-01-01') + INTERVAL 1 DAY;
SELECT toNullable(toDate('2020-01-01')) + INTERVAL 1 DAY;
SELECT toLowCardinality(toDate('2020-01-01')) + INTERVAL 1 DAY;
SELECT materialize(toDate('2020-01-01')) - INTERVAL 1 MONTH;
SELECT toDateTime('2020-01-01 00:00:00', 'UTC') + INTERVAL 1 HOUR;
SELECT materialize(toNullable(toDateTime('2020-01-01 00:00:00', 'UTC'))) + INTERVAL 90 MINUTE;

-- Date plus a tuple of intervals.
SELECT toDate('2020-01-01') + (INTERVAL 1 DAY, INTERVAL 1 MONTH);

-- Interval plus interval (merge into a tuple of intervals).
SELECT INTERVAL 1 DAY + INTERVAL 1 HOUR;

-- Tuple arithmetic and tuple-and-number arithmetic, plain and materialized.
SELECT (1, 2, 3) + (4, 5, 6);
SELECT (1, 2) - (3, 4);
SELECT (10, 20) * 2;
SELECT materialize((1, 2)) + (3, 4);

-- Array arithmetic re-evaluates the operation on the element types (a separate code path that
-- delegates to a sibling function built for those element types).
SELECT [1, 2, 3] + [4, 5, 6];
SELECT materialize([1, 2, 3]) + materialize([4, 5, 6]);
SELECT [1, 2, 3] * 2;
SELECT [toDate('2020-01-01'), toDate('2020-06-01')] + [toIntervalDay(1), toIntervalDay(2)];
SELECT [[1, 2], [3]] + [[10, 20], [30]];

-- Arrays whose element-level operation is a tuple-family special case (see the scope note above:
-- these still pin the build-time context through the sibling's cached tuple sub-functions).
SELECT [(1, 2)] + [(3, 4)];
SELECT [INTERVAL 1 DAY] + [INTERVAL 1 HOUR];

-- Plain numeric arithmetic (no special case applies; the context is not touched at all).
SELECT number + 1, number * 2, number - 3 FROM numbers(3);

-- Regression: a sorting key that contains arithmetic is stored in the table metadata with the
-- expression built by the CREATE query. It must still be executable during a background merge,
-- when that query context no longer exists.
DROP TABLE IF EXISTS t_arith_key;
CREATE TABLE t_arith_key (a Int64, b Int64, d Date) ENGINE = MergeTree ORDER BY (a + b);
INSERT INTO t_arith_key VALUES (3, 4, '2020-01-01');
INSERT INTO t_arith_key VALUES (1, 1, '2020-01-02');
OPTIMIZE TABLE t_arith_key FINAL;
SELECT a, b, d FROM t_arith_key ORDER BY a, b;
DROP TABLE t_arith_key;

-- Regression: the same, for the interval path: the sorting key `d + INTERVAL 1 DAY` executes
-- through the interval sub-function resolved when the expression was built by the CREATE query
-- (prepared_interval_function), and must still be executable during a merge after that query
-- context no longer exists.
DROP TABLE IF EXISTS t_arith_key_interval;
CREATE TABLE t_arith_key_interval (d Date, v Int64) ENGINE = MergeTree ORDER BY (d + INTERVAL 1 DAY);
INSERT INTO t_arith_key_interval VALUES ('2020-01-02', 1);
INSERT INTO t_arith_key_interval VALUES ('2020-01-01', 2);
OPTIMIZE TABLE t_arith_key_interval FINAL;
SELECT d, v FROM t_arith_key_interval ORDER BY d;
DROP TABLE t_arith_key_interval;

-- Regression: the same, for the array path: the sorting key `arr + [10, 20]` executes through the
-- element-level sibling function resolved when the expression was built by the CREATE query
-- (array_element_function), and must still be executable during a merge after that query
-- context no longer exists.
DROP TABLE IF EXISTS t_arith_key_array;
CREATE TABLE t_arith_key_array (arr Array(UInt32), v Int64) ENGINE = MergeTree ORDER BY (arr + [10, 20]);
INSERT INTO t_arith_key_array VALUES ([3, 4], 1);
INSERT INTO t_arith_key_array VALUES ([1, 2], 2);
OPTIMIZE TABLE t_arith_key_array FINAL;
SELECT arr, v FROM t_arith_key_array ORDER BY arr;
DROP TABLE t_arith_key_array;

-- Regression: getReturnTypeImpl is re-invoked from IFunction::compile when a stored arithmetic
-- expression (here the sorting key a + k + 1) is JIT-compiled at pipeline-build time. After an ALTER
-- rebinds the table metadata to a query context that then dies, that expression must still be
-- type-resolvable. Before the fix this raised `Context has expired` (LOGICAL_ERROR).
DROP TABLE IF EXISTS t_arith_key_jit;
CREATE TABLE t_arith_key_jit (k Int64, a Nullable(Int64), v Int64)
ENGINE = ReplacingMergeTree ORDER BY (k, a + k + 1)
SETTINGS allow_nullable_key = 1;
INSERT INTO t_arith_key_jit VALUES (1, 1, 1);
INSERT INTO t_arith_key_jit VALUES (1, 1, 2);
ALTER TABLE t_arith_key_jit ADD COLUMN c Int64; -- rebinds metadata to the ALTER query context, which then dies
SELECT k, a FROM t_arith_key_jit FINAL WHERE k >= 0
SETTINGS query_plan_optimize_lazy_final = 1, compile_expressions = 1,
         min_count_to_compile_expression = 0, min_filtered_ratio_for_lazy_final = 0;
DROP TABLE t_arith_key_jit;
