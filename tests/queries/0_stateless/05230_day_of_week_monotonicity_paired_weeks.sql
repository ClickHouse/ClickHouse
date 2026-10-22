SET session_timezone = 'UTC';

-- `toDayOfWeek` is monotonic on a range iff the range stays inside one week in the ordering of its
-- `week_mode`: a Monday-week for modes 0 and 1, a Sunday-week for modes 2 and 3. The mode is not
-- visible to the monotonicity analysis, so the factor that decides it pairs both weeks: a range is
-- monotonic iff it stays inside one Monday-to-Saturday run or on one Sunday. That keeps the key usable
-- for ranges that span several days, which a granule of a sparse table does.

DROP TABLE IF EXISTS t_day_of_week_paired;

CREATE TABLE t_day_of_week_paired (d Date) ENGINE = MergeTree ORDER BY d
SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;

-- 2026-08-03 is a Monday; one row per day up to the Sunday, one granule per row.
INSERT INTO t_day_of_week_paired SELECT toDate('2026-08-03') + number FROM numbers(7);

-- The key ranges are [Mon, Tue], [Tue, Wed], ..., [Fri, Sat], [Sat, Sun] and [Sun, Sun]. Looking for
-- the Wednesday keeps the two ranges that contain it plus [Sat, Sun], where the factor differs, so three
-- of the seven granules are read - in every mode.
SELECT count() FROM t_day_of_week_paired WHERE toDayOfWeek(d) = 3 SETTINGS max_rows_to_read = 3;
SELECT count() FROM t_day_of_week_paired WHERE toDayOfWeek(d, 1) = 2 SETTINGS max_rows_to_read = 3;
SELECT count() FROM t_day_of_week_paired WHERE toDayOfWeek(d, 2) = 3 SETTINGS max_rows_to_read = 3;
SELECT count() FROM t_day_of_week_paired WHERE toDayOfWeek(d, 3) = 4 SETTINGS max_rows_to_read = 3;

-- Looking for the Sunday prunes every range that stays inside Monday to Saturday and reads the two
-- that hold it.
SELECT count() FROM t_day_of_week_paired WHERE toDayOfWeek(d) = 7 SETTINGS max_rows_to_read = 2;
SELECT count() FROM t_day_of_week_paired WHERE toDayOfWeek(d, 1) = 6 SETTINGS max_rows_to_read = 2;
SELECT count() FROM t_day_of_week_paired WHERE toDayOfWeek(d, 2) = 0 SETTINGS max_rows_to_read = 2;
SELECT count() FROM t_day_of_week_paired WHERE toDayOfWeek(d, 3) = 1 SETTINGS max_rows_to_read = 2;

-- And the Monday and Saturday, at the two ends of the run, are still found in the Sunday-first modes,
-- where the range [Sat, Sun] runs backwards and must not be mapped to an empty range.
SELECT count() FROM t_day_of_week_paired WHERE toDayOfWeek(d, 2) = 1;
SELECT count() FROM t_day_of_week_paired WHERE toDayOfWeek(d, 3) = 2;
SELECT count() FROM t_day_of_week_paired WHERE toDayOfWeek(d, 2) = 6;
SELECT count() FROM t_day_of_week_paired WHERE toDayOfWeek(d, 3) = 7;

DROP TABLE t_day_of_week_paired;
