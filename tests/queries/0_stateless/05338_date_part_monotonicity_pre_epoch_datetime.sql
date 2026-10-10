SET session_timezone = 'UTC';

-- A `DateTime` in a time zone west of UTC starts on the local day 1969-12-31. The factor deciding the
-- monotonicity of these functions saturated that day to 1970-01-01, the start of the next day, month and
-- year, so a key range crossing it was claimed monotonic and its granules were pruned.

DROP TABLE IF EXISTS t_date_part_pre_epoch;

CREATE TABLE t_date_part_pre_epoch (dt DateTime('America/New_York')) ENGINE = MergeTree ORDER BY dt
SETTINGS index_granularity = 8, add_minmax_index_for_numeric_columns = 0;

INSERT INTO t_date_part_pre_epoch SELECT toDateTime(number * 3600) FROM numbers(2000);

SELECT count() FROM t_date_part_pre_epoch WHERE toMonth(dt) = 1;
SELECT count() FROM t_date_part_pre_epoch WHERE toQuarter(dt) = 1;
SELECT count() FROM t_date_part_pre_epoch WHERE toDayOfYear(dt) = 1;
SELECT count() FROM t_date_part_pre_epoch WHERE toDayOfMonth(dt) = 1;
SELECT count() FROM t_date_part_pre_epoch WHERE toDayOfMonth(dt) IN (1, 3);
-- A coarser exclusion search hands the same reversed range to `MergeTreeSetIndex`, which checks its order.
SELECT count() FROM t_date_part_pre_epoch WHERE toDayOfMonth(dt) IN (1, 3) SETTINGS merge_tree_coarse_index_granularity = 20;
SELECT count() FROM t_date_part_pre_epoch WHERE toHour(dt) = 20;
SELECT count() FROM t_date_part_pre_epoch WHERE toTimeWithFixedDate(dt) = toDateTime('1970-01-02 00:00:00', 'America/New_York');

DROP TABLE t_date_part_pre_epoch;

-- The same through a `Nullable` key. The session time zone equals the column's one, so only the factor is tested.

SET session_timezone = 'America/New_York';

DROP TABLE IF EXISTS t_date_part_pre_epoch_nullable;

CREATE TABLE t_date_part_pre_epoch_nullable (dt Nullable(DateTime('America/New_York'))) ENGINE = MergeTree ORDER BY dt
SETTINGS index_granularity = 8, allow_nullable_key = 1, add_minmax_index_for_numeric_columns = 0;

INSERT INTO t_date_part_pre_epoch_nullable SELECT toDateTime(number * 3600) FROM numbers(2000);

SELECT count() FROM t_date_part_pre_epoch_nullable WHERE toDayOfMonth(dt) IN (1, 3);

DROP TABLE t_date_part_pre_epoch_nullable;

SET session_timezone = 'UTC';

-- The `toHour` factor narrowed a `DateTime64` day number to `UInt16`: 1970-01-01 and 2149-06-07 shared it.

DROP TABLE IF EXISTS t_hour_datetime64_alias;

CREATE TABLE t_hour_datetime64_alias (dt DateTime64(0, 'UTC')) ENGINE = MergeTree ORDER BY dt
SETTINGS index_granularity = 8, add_minmax_index_for_numeric_columns = 0;

INSERT INTO t_hour_datetime64_alias VALUES ('1970-01-01 20:00:00'), ('1970-01-02 21:00:00'), ('2149-06-07 20:00:00');

SELECT count() FROM t_hour_datetime64_alias WHERE toHour(dt) = 21;

DROP TABLE t_hour_datetime64_alias;

-- `toHour` rounds a pre-epoch `DateTime64` down to whole seconds, so its factor has to as well.

DROP TABLE IF EXISTS t_hour_datetime64_fraction;

CREATE TABLE t_hour_datetime64_fraction (dt DateTime64(3, 'UTC')) ENGINE = MergeTree ORDER BY dt
SETTINGS index_granularity = 8, add_minmax_index_for_numeric_columns = 0;

INSERT INTO t_hour_datetime64_fraction VALUES ('1969-12-31 23:59:59.500'), ('1970-01-01 00:00:00.500'), ('1970-01-01 00:30:00.000');

SELECT count() FROM t_hour_datetime64_fraction WHERE toHour(dt) = 0;
SELECT count() FROM t_hour_datetime64_fraction WHERE toHour(dt) = 23;

DROP TABLE t_hour_datetime64_fraction;

-- `toTimeWithFixedDate` truncates a pre-epoch `DateTime64` towards zero, so its factor has to as well:
-- the two rows below give 1970-01-02 23:59:59 and 1970-01-02 00:00:00.

DROP TABLE IF EXISTS t_time_datetime64_fraction;

CREATE TABLE t_time_datetime64_fraction (dt DateTime64(3, 'UTC')) ENGINE = MergeTree ORDER BY dt
SETTINGS index_granularity = 8, add_minmax_index_for_numeric_columns = 0;

INSERT INTO t_time_datetime64_fraction VALUES ('1969-12-31 23:59:58.500'), ('1969-12-31 23:59:59.500');

SELECT count() FROM t_time_datetime64_fraction WHERE toTimeWithFixedDate(dt) = toDateTime('1970-01-02 00:00:00', 'UTC');

DROP TABLE t_time_datetime64_fraction;
