-- A container key such as `arr::String` cannot be probed by `castColumnAccurateOrNull`, so the constant
-- used to be rendered from its own type instead of the key column's: a `DateTime64(6)` element gives six
-- fractional digits where the key space of `Array(DateTime64(3))` holds three, and the part is pruned.

DROP TABLE IF EXISTS t_scale_array_cast_key;

CREATE TABLE t_scale_array_cast_key (arr Array(DateTime64(3, 'UTC'))) ENGINE = MergeTree ORDER BY arr::String;
INSERT INTO t_scale_array_cast_key VALUES ([toDateTime64(1675252800, 3, 'UTC')]);

SELECT count() FROM t_scale_array_cast_key WHERE arr = [toDateTime64(1675252800, 6, 'UTC')];
SELECT count() FROM t_scale_array_cast_key WHERE arr = [toDateTime64(1675252800, 3, 'UTC')];
SELECT count() FROM t_scale_array_cast_key WHERE arr = [toDateTime64(1675252800, 0, 'UTC')];
SELECT count() FROM t_scale_array_cast_key WHERE arr = [toDateTime64('2023-02-01 12:00:00.000001', 6, 'UTC')];
SELECT count() FROM t_scale_array_cast_key WHERE arr IN ([toDateTime64(1675252800, 6, 'UTC')], [toDateTime64('2023-02-01 12:00:00.000001', 6, 'UTC')]);

DROP TABLE t_scale_array_cast_key;

-- Every partition of `PARTITION BY arr::String` has one value, so a missed rendering prunes it whole.

CREATE TABLE t_scale_array_cast_key (arr Array(DateTime64(3, 'UTC'))) ENGINE = MergeTree PARTITION BY arr::String ORDER BY tuple();
INSERT INTO t_scale_array_cast_key VALUES ([toDateTime64(1675252800, 3, 'UTC')]);

SELECT count() FROM t_scale_array_cast_key WHERE arr = [toDateTime64(1675252800, 6, 'UTC')];
SELECT count() FROM t_scale_array_cast_key WHERE arr = [toDateTime64(1675252800, 3, 'UTC')];

DROP TABLE t_scale_array_cast_key;

-- The row at the maximum key of a part is the one a missed rendering loses.

CREATE TABLE t_scale_array_cast_key (arr Array(DateTime64(3, 'UTC')), v UInt64) ENGINE = MergeTree ORDER BY arr::String;
INSERT INTO t_scale_array_cast_key SELECT [toDateTime64(1675252800 + number, 3, 'UTC')], number FROM numbers(10000);

SELECT count() FROM t_scale_array_cast_key WHERE arr = [toDateTime64(1675262799, 6, 'UTC')];
SELECT count() FROM t_scale_array_cast_key WHERE arr = [toDateTime64(1675262799, 3, 'UTC')];
SELECT count() FROM t_scale_array_cast_key WHERE arr = [toDateTime64(1675252800, 6, 'UTC')];

DROP TABLE t_scale_array_cast_key;

-- `Tuple` cannot be probed either.

CREATE TABLE t_scale_array_cast_key (t Tuple(DateTime64(3, 'UTC'))) ENGINE = MergeTree ORDER BY t::String;
INSERT INTO t_scale_array_cast_key VALUES (tuple(toDateTime64(1675252800, 3, 'UTC')));

SELECT count() FROM t_scale_array_cast_key WHERE t = tuple(toDateTime64(1675252800, 6, 'UTC'));
SELECT count() FROM t_scale_array_cast_key WHERE t = tuple(toDateTime64(1675252800, 3, 'UTC'));
SELECT count() FROM t_scale_array_cast_key WHERE t = tuple(toDateTime64('2023-02-01 12:00:00.000001', 6, 'UTC'));

DROP TABLE t_scale_array_cast_key;
