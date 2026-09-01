-- The planner only bounds-checks `max_threads * max_streams_to_max_threads_ratio`, but `StorageMerge`
-- multiplies the result by `max_streams_multiplier_for_merge_tables` (clamped to the number of selected
-- tables). The product used to be cast to `size_t` unchecked, which is undefined behavior when it
-- exceeds the range of `size_t`. Now it throws `PARAMETER_OUT_OF_BOUND`.
DROP TABLE IF EXISTS t_streams_mult_1;
DROP TABLE IF EXISTS t_streams_mult_2;
DROP TABLE IF EXISTS t_streams_mult_3;
DROP TABLE IF EXISTS t_streams_mult_4;
CREATE TABLE t_streams_mult_1 (id Int32) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_streams_mult_2 (id Int32) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_streams_mult_3 (id Int32) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_streams_mult_4 (id Int32) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_streams_mult_1 SELECT number FROM numbers(10);
INSERT INTO t_streams_mult_2 SELECT number FROM numbers(10);
INSERT INTO t_streams_mult_3 SELECT number FROM numbers(10);
INSERT INTO t_streams_mult_4 SELECT number FROM numbers(10);

-- `max_streams` = 4 * 2^60 = 2^62 passes the planner's check, but multiplying it by 4 selected tables
-- gives 2^64, which does not fit into `size_t`.
SELECT count() FROM merge(currentDatabase(), '^t_streams_mult_') GROUP BY id
SETTINGS max_streams_to_max_threads_ratio = 1152921504606846976, max_threads = 4, max_streams_multiplier_for_merge_tables = 4; -- { serverError PARAMETER_OUT_OF_BOUND }

-- A representable product passes the check and the query works.
SELECT count() FROM merge(currentDatabase(), '^t_streams_mult_') GROUP BY id FORMAT Null
SETTINGS max_streams_to_max_threads_ratio = 1024, max_threads = 4, max_streams_multiplier_for_merge_tables = 4;

DROP TABLE t_streams_mult_1;
DROP TABLE t_streams_mult_2;
DROP TABLE t_streams_mult_3;
DROP TABLE t_streams_mult_4;
