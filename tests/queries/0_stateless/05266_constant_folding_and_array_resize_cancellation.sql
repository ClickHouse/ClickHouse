-- A query that runs out of time stops inside arrayResize and before evaluating its next constant expression.
-- A query that keeps evaluating after its deadline reports the division by zero instead.

SELECT intDiv(length(arrayResize([], 500000000)), 0) SETTINGS max_execution_time = 0.1, timeout_overflow_mode = 'throw'; -- { serverError TIMEOUT_EXCEEDED }

-- A function call has no partial result, so it fails in the break mode too, extending to the right and to the left.
SELECT intDiv(length(arrayResize([], 500000000)), 0) SETTINGS max_execution_time = 0.1, timeout_overflow_mode = 'break'; -- { serverError TIMEOUT_EXCEEDED }
SELECT intDiv(length(arrayResize([], -500000000)), 0) SETTINGS max_execution_time = 0.1, timeout_overflow_mode = 'break'; -- { serverError TIMEOUT_EXCEEDED }
-- Copies of a large extender are counted by their size, so a few thousand of them stop at the deadline too.
SELECT intDiv(length(arrayResize([], 8000, repeat('x', 16384))), 0) SETTINGS max_execution_time = 0.001, timeout_overflow_mode = 'break'; -- { serverError TIMEOUT_EXCEEDED }
-- So do many rows that each extend an array by 60000 elements.
SELECT intDiv(length(arrayMap(x -> arrayResize(CAST([] AS Array(UInt8)), 60000 - x % 2), range(5000))), 0) SETTINGS max_execution_time = 0.1, timeout_overflow_mode = 'break'; -- { serverError TIMEOUT_EXCEEDED }

-- The edit distance of two 20000-character strings is a constant that takes longer than the deadline to evaluate.
SELECT editDistance(repeat('a', 20000), repeat('b', 20000)), intDiv(1, 0) SETTINGS max_execution_time = 0.001, timeout_overflow_mode = 'throw'; -- { serverError TIMEOUT_EXCEEDED }
-- The next function is not built after the deadline either, with constant or other arguments or with its parameters, nor
-- an aggregate function whose parameter passed it, nor a function after its lambda; otherwise it reports the unknown
-- tokenizer, the illegal argument type, the non-constant or out-of-bound parameter or the illegal lambda result type.
SELECT editDistance(repeat('a', 20000), repeat('b', 20000)), hasPhrase('a', 'a', 'invalid') SETTINGS max_execution_time = 0.001, timeout_overflow_mode = 'throw'; -- { serverError TIMEOUT_EXCEEDED }
SELECT editDistance(repeat('a', 20000), repeat('b', 20000)), hasPhrase(toString(dummy), 'a', 'invalid') FROM system.one SETTINGS max_execution_time = 0.001, timeout_overflow_mode = 'throw'; -- { serverError TIMEOUT_EXCEEDED }
SELECT editDistance(repeat('a', 20000), repeat('b', 20000)), quantile(number)(number) FROM numbers(1) SETTINGS max_execution_time = 0.001, timeout_overflow_mode = 'throw'; -- { serverError TIMEOUT_EXCEEDED }
SELECT quantile(editDistance(repeat('a', 20000), repeat('b', 20000)))(number) FROM numbers(1) SETTINGS max_execution_time = 0.001, timeout_overflow_mode = 'throw'; -- { serverError TIMEOUT_EXCEEDED }
SELECT arrayFilter(x -> editDistance(repeat('a', 20000), repeat('b', 20000)), [1]) SETTINGS max_execution_time = 0.001, timeout_overflow_mode = 'throw'; -- { serverError TIMEOUT_EXCEEDED }
-- A constant of more than 1 MiB is not kept by the analyzer, so it is evaluated again when the query plan is built.
SELECT intDiv(length(repeat('ab', 600000)), 0), editDistance(repeat('a', 20000), repeat('b', 20000)) SETTINGS max_execution_time = 0.001, timeout_overflow_mode = 'throw'; -- { serverError TIMEOUT_EXCEEDED }

-- A constant reference vector is cast to the type of the QBit column during analysis, which fails to parse 'x';
-- after the deadline, the query stops before that cast.
DROP TABLE IF EXISTS t_qbit;
CREATE TABLE t_qbit (q QBit(Float32, 4)) ENGINE = MergeTree ORDER BY tuple();
SELECT L2DistanceTransposed(q, ['x'], 16) FROM t_qbit SETTINGS optimize_qbit_distance_function_reads = 1; -- { serverError CANNOT_PARSE_NUMBER }
SELECT L2DistanceTransposed(q, ['x'], 16), editDistance(repeat('a', 20000), repeat('b', 20000)) FROM t_qbit SETTINGS max_execution_time = 0.001, timeout_overflow_mode = 'throw', optimize_qbit_distance_function_reads = 1; -- { serverError TIMEOUT_EXCEEDED }
DROP TABLE t_qbit;

-- A timeout while folding a partition constant for index analysis is not one of the ignored evaluation failures,
-- in the break mode too; otherwise the query completes after folding the predicate for every partition.
DROP TABLE IF EXISTS t_partition_fold;
CREATE TABLE t_partition_fold (p UInt8) ENGINE = MergeTree PARTITION BY p ORDER BY tuple();
INSERT INTO t_partition_fold SELECT number FROM numbers(10);
SELECT count() FROM t_partition_fold WHERE length(arrayResize([], 8000, repeat(toString(p), 16384))) = editDistance(repeat('a', 20000), repeat('b', 20000)) SETTINGS use_constant_folding_in_index_analysis = 1, optimize_move_to_prewhere = 0, query_plan_optimize_prewhere = 0, max_execution_time = 0.001, timeout_overflow_mode = 'break'; -- { serverError TIMEOUT_EXCEEDED }
DROP TABLE t_partition_fold;

-- The same holds for the filter that rewriting an outer JOIN evaluates for not-matched and for matched rows,
-- and for a constant that index analysis transforms with the primary key expression.
DROP TABLE IF EXISTS t_join_fold;
CREATE TABLE t_join_fold (k UInt64, s String) ENGINE = MergeTree ORDER BY k;
SELECT count() FROM t_join_fold AS l LEFT JOIN t_join_fold AS r ON l.k = r.k WHERE length(arrayResize([], 8000, repeat(r.s || 'x', 16384))) = 0 SETTINGS query_plan_convert_outer_join_to_inner_join = 1, max_execution_time = 0.001, timeout_overflow_mode = 'break', join_use_nulls = 0; -- { serverError TIMEOUT_EXCEEDED }
SELECT count() FROM t_join_fold AS l ANY LEFT JOIN (SELECT k, arrayResize(materialize([]), 8000, materialize(repeat('x', 16384))) AS a FROM t_join_fold) AS r ON l.k = r.k WHERE length(r.a) = 0 SETTINGS query_plan_convert_outer_join_to_inner_join = 0, query_plan_convert_any_join_to_semi_or_anti_join = 1, max_execution_time = 0.001, timeout_overflow_mode = 'break', join_use_nulls = 0; -- { serverError TIMEOUT_EXCEEDED }
DROP TABLE t_join_fold;

DROP TABLE IF EXISTS t_key_fold;
CREATE TABLE t_key_fold (p UInt16) ENGINE = MergeTree ORDER BY length(arrayResize([], p, repeat('x', 16384)));
INSERT INTO t_key_fold VALUES (1);
SELECT count() FROM t_key_fold WHERE p = 8000 SETTINGS max_execution_time = 0.001, timeout_overflow_mode = 'break'; -- { serverError TIMEOUT_EXCEEDED }
DROP TABLE t_key_fold;
