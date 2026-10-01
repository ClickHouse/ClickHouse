-- A query that runs out of time stops inside arrayResize and before evaluating its next constant expression.
-- A query that keeps evaluating after its deadline reports the division by zero instead.

SELECT intDiv(length(arrayResize([], 500000000)), 0) SETTINGS max_execution_time = 0.1, timeout_overflow_mode = 'throw'; -- { serverError TIMEOUT_EXCEEDED }

-- A function call has no partial result, so it fails in the break mode too, extending to the right and to the left.
SELECT intDiv(length(arrayResize([], 500000000)), 0) SETTINGS max_execution_time = 0.1, timeout_overflow_mode = 'break'; -- { serverError TIMEOUT_EXCEEDED }
SELECT intDiv(length(arrayResize([], -500000000)), 0) SETTINGS max_execution_time = 0.1, timeout_overflow_mode = 'break'; -- { serverError TIMEOUT_EXCEEDED }
-- Copies of a large extender are counted by their size, so a few thousand of them stop at the deadline too.
SELECT intDiv(length(arrayResize([], 8000, repeat('x', 16384))), 0) SETTINGS max_execution_time = 0.001, timeout_overflow_mode = 'break'; -- { serverError TIMEOUT_EXCEEDED }

-- The edit distance of two 20000-character strings is a constant that takes longer than the deadline to evaluate.
SELECT editDistance(repeat('a', 20000), repeat('b', 20000)), intDiv(1, 0) SETTINGS max_execution_time = 0.001, timeout_overflow_mode = 'throw'; -- { serverError TIMEOUT_EXCEEDED }
-- The next function is not built after the deadline either, with constant or other arguments; otherwise it reports the unknown tokenizer.
SELECT editDistance(repeat('a', 20000), repeat('b', 20000)), hasPhrase('a', 'a', 'invalid') SETTINGS max_execution_time = 0.001, timeout_overflow_mode = 'throw'; -- { serverError TIMEOUT_EXCEEDED }
SELECT editDistance(repeat('a', 20000), repeat('b', 20000)), hasPhrase(toString(dummy), 'a', 'invalid') FROM system.one SETTINGS max_execution_time = 0.001, timeout_overflow_mode = 'throw'; -- { serverError TIMEOUT_EXCEEDED }
-- A constant of more than 1 MiB is not kept by the analyzer, so it is evaluated again when the query plan is built.
SELECT intDiv(length(repeat('ab', 600000)), 0), editDistance(repeat('a', 20000), repeat('b', 20000)) SETTINGS max_execution_time = 0.001, timeout_overflow_mode = 'throw'; -- { serverError TIMEOUT_EXCEEDED }

-- A constant reference vector is cast to the type of the QBit column during analysis, which fails to parse 'x';
-- after the deadline, the query stops before that cast.
DROP TABLE IF EXISTS t_qbit;
CREATE TABLE t_qbit (q QBit(Float32, 4)) ENGINE = MergeTree ORDER BY tuple();
SELECT L2DistanceTransposed(q, ['x'], 16) FROM t_qbit SETTINGS optimize_qbit_distance_function_reads = 1; -- { serverError CANNOT_PARSE_NUMBER }
SELECT editDistance(repeat('a', 20000), repeat('b', 20000)), L2DistanceTransposed(q, ['x'], 16) FROM t_qbit SETTINGS max_execution_time = 0.001, timeout_overflow_mode = 'throw', optimize_qbit_distance_function_reads = 1; -- { serverError TIMEOUT_EXCEEDED }
DROP TABLE t_qbit;
