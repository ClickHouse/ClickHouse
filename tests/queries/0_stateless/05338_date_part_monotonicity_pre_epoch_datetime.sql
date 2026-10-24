SET session_timezone = 'UTC';

-- A `DateTime` in a time zone west of UTC starts on the local day 1969-12-31. The factor deciding the
-- monotonicity of these functions saturated that day to 1970-01-01, the start of the next month and
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

DROP TABLE t_date_part_pre_epoch;

-- The same through a `Nullable` key. The session time zone equals the column's one, so only the factor is tested.

SET session_timezone = 'America/New_York';

DROP TABLE IF EXISTS t_date_part_pre_epoch_nullable;

CREATE TABLE t_date_part_pre_epoch_nullable (dt Nullable(DateTime('America/New_York'))) ENGINE = MergeTree ORDER BY dt
SETTINGS index_granularity = 8, allow_nullable_key = 1, add_minmax_index_for_numeric_columns = 0;

INSERT INTO t_date_part_pre_epoch_nullable SELECT toDateTime(number * 3600) FROM numbers(2000);

SELECT count() FROM t_date_part_pre_epoch_nullable WHERE toDayOfMonth(dt) IN (1, 3);

DROP TABLE t_date_part_pre_epoch_nullable;
