-- Test: exercises `max_streams * max_streams_to_max_threads_ratio` POSITIVE overflow path.
-- Covers: src/Interpreters/InterpreterSelectQuery.cpp:2798-2806 (legacy planner) and
--         src/Planner/PlannerJoinTree.cpp:848-856 (new analyzer).
-- The existing test 03148 only covers the NEGATIVE overflow path
-- (max_streams_to_max_threads_ratio = -9223372036854775808 -> x < lowest()).
-- This test covers the POSITIVE overflow path (x > size_t::max()) which was the
-- ORIGINAL AST fuzzer crash: "inf is outside the range of representable values of type 'unsigned long'".
DROP TABLE IF EXISTS test_pos_overflow;
CREATE TABLE test_pos_overflow
(
    id UInt64,
    value String
) ENGINE = MergeTree ORDER BY id;

INSERT INTO test_pos_overflow VALUES (0, 'Value_0');

SELECT * FROM test_pos_overflow SETTINGS max_threads = 1025, max_streams_to_max_threads_ratio = 1e30, enable_analyzer = 1; -- { serverError PARAMETER_OUT_OF_BOUND }

SELECT * FROM test_pos_overflow SETTINGS max_threads = 1025, max_streams_to_max_threads_ratio = 1e30, enable_analyzer = 0; -- { serverError PARAMETER_OUT_OF_BOUND }

DROP TABLE test_pos_overflow;
