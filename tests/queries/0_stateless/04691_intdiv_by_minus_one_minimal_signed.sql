-- `intDiv` of the minimal signed number by the constant -1 must be an `ILLEGAL_DIVISION` exception
-- in the vectorized by-constant path, same as in the constant-folded and generic (both arguments
-- non-constant) paths. It used to wrap silently to the minimal signed number, and because the
-- read-in-order optimization treats `intDiv` by a negative constant as monotonically decreasing,
-- the wrapped value ended up out of order in a stream assumed to be sorted: a logical error
-- 'Equal values are not contiguous within the range assumed to be sorted' in
-- `DistinctSortedStreamTransform` in debug builds, wrong query results in release builds.

SELECT intDiv(materialize(toInt64(-9223372036854775808)), -1); -- { serverError ILLEGAL_DIVISION }
SELECT intDiv(toInt64(-9223372036854775808), -1); -- { serverError ILLEGAL_DIVISION }
SELECT intDiv(materialize(toInt32(-2147483648)), -1); -- { serverError ILLEGAL_DIVISION }
SELECT intDiv(toInt32(-2147483648), -1); -- { serverError ILLEGAL_DIVISION }
SELECT intDiv(materialize(toInt64(-9223372036854775808)), toInt64(-1)); -- { serverError ILLEGAL_DIVISION }

-- Values next to the minimum divide fine, and `intDivOrZero` keeps returning zero.
SELECT intDiv(materialize(toInt64(-9223372036854775807)), -1);
SELECT intDiv(materialize(toInt32(-2147483647)), -1);
SELECT intDivOrZero(materialize(toInt64(-9223372036854775808)), -1);

-- Pin the settings that plan the in-order DISTINCT; the test must not depend on their randomization.
SET optimize_distinct_in_order = 1;
SET optimize_read_in_order = 1;
SET max_threads = 4;

-- The scenario from the stress test: a sorted in-order read through the monotonic `intDiv(x, -1)`
-- with the minimal signed number in the data must fail with an exception instead of feeding an
-- unsorted stream into DISTINCT.
DROP TABLE IF EXISTS t_intdiv_min;
CREATE TABLE t_intdiv_min (x Int64) ENGINE = MergeTree ORDER BY x;
SYSTEM STOP MERGES t_intdiv_min;
INSERT INTO t_intdiv_min SELECT number FROM numbers(100);
INSERT INTO t_intdiv_min SELECT number FROM numbers(100);
INSERT INTO t_intdiv_min SELECT number + 9223372036854775806 FROM numbers(100);

SELECT DISTINCT intDiv(x, -1) AS d FROM t_intdiv_min ORDER BY d ASC FORMAT Null; -- { serverError ILLEGAL_DIVISION }

DROP TABLE t_intdiv_min;

-- Without the minimal signed number the same query works and the result is sorted.
DROP TABLE IF EXISTS t_intdiv_no_min;
CREATE TABLE t_intdiv_no_min (x Int64) ENGINE = MergeTree ORDER BY x;
SYSTEM STOP MERGES t_intdiv_no_min;
INSERT INTO t_intdiv_no_min SELECT number FROM numbers(100);
INSERT INTO t_intdiv_no_min SELECT number FROM numbers(100);

SELECT groupArray(d) = arraySort(groupArray(d)), count() FROM
(
    SELECT DISTINCT intDiv(x, -1) AS d FROM t_intdiv_no_min ORDER BY d ASC
);

DROP TABLE t_intdiv_no_min;
