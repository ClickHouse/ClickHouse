-- A `DateTime64` constant of a scale other than the column's must not change which parts and
-- granules the index analysis selects: the key space of `d::String` holds the digits of the column's
-- scale, so a constant rendered at its own scale misses it.

DROP TABLE IF EXISTS t_scale_cast_key;

CREATE TABLE t_scale_cast_key (d DateTime64(3, 'UTC')) ENGINE = MergeTree ORDER BY d::String;
INSERT INTO t_scale_cast_key VALUES (toDateTime64(1675252800, 3, 'UTC'));

SELECT count() FROM t_scale_cast_key WHERE d = toDateTime64(1675252800, 6, 'UTC');
SELECT count() FROM t_scale_cast_key WHERE d = toDateTime64(1675252800, 3, 'UTC');
SELECT count() FROM t_scale_cast_key WHERE d = toDateTime64(1675252800, 0, 'UTC');
SELECT count() FROM t_scale_cast_key WHERE d = toDateTime64('2023-02-01 12:00:00.000001', 6, 'UTC');

DROP TABLE t_scale_cast_key;

-- Every partition of `PARTITION BY d::String` has one value, so a missed rendering prunes it whole.

CREATE TABLE t_scale_cast_key (d DateTime64(3, 'UTC')) ENGINE = MergeTree PARTITION BY d::String ORDER BY tuple();
INSERT INTO t_scale_cast_key VALUES (toDateTime64(1675252800, 3, 'UTC'));

SELECT count() FROM t_scale_cast_key WHERE d = toDateTime64(1675252800, 6, 'UTC');
SELECT count() FROM t_scale_cast_key WHERE d = toDateTime64(1675252800, 3, 'UTC');

DROP TABLE t_scale_cast_key;

-- The other direction: a finer column with a coarser constant.

CREATE TABLE t_scale_cast_key (d DateTime64(6, 'UTC')) ENGINE = MergeTree ORDER BY d::String;
INSERT INTO t_scale_cast_key VALUES (toDateTime64(1675252800, 6, 'UTC'));

SELECT count() FROM t_scale_cast_key WHERE d = toDateTime64(1675252800, 3, 'UTC');
SELECT count() FROM t_scale_cast_key WHERE d = toDateTime64(1675252800, 6, 'UTC');

DROP TABLE t_scale_cast_key;

-- The row at the maximum key of a part is the one a missed rendering loses.

CREATE TABLE t_scale_cast_key (d DateTime64(3, 'UTC'), v UInt64) ENGINE = MergeTree ORDER BY d::String;
INSERT INTO t_scale_cast_key SELECT toDateTime64(1675252800 + number, 3, 'UTC'), number FROM numbers(10000);

SELECT count() FROM t_scale_cast_key WHERE d = toDateTime64(1675262799, 6, 'UTC');
SELECT count() FROM t_scale_cast_key WHERE d = toDateTime64(1675262799, 3, 'UTC');
SELECT count() FROM t_scale_cast_key WHERE d = toDateTime64(1675252800, 6, 'UTC');

DROP TABLE t_scale_cast_key;

-- A set with one element the key type cannot represent must not drag the representable ones into the
-- constant's own value space.

CREATE TABLE t_scale_cast_key (d DateTime64(3, 'UTC')) ENGINE = MergeTree ORDER BY d::String;
INSERT INTO t_scale_cast_key VALUES (toDateTime64(1675252800, 3, 'UTC'));

SELECT count() FROM t_scale_cast_key
WHERE d IN (toDateTime64(1675252800, 6, 'UTC'), toDateTime64('2023-02-01 12:00:00.000001', 6, 'UTC'));

SELECT count() FROM t_scale_cast_key
WHERE d NOT IN (toDateTime64(1675252800, 6, 'UTC'), toDateTime64('2023-02-01 12:00:00.000001', 6, 'UTC'));

SELECT count() FROM t_scale_cast_key
WHERE has([toDateTime64(1675252800, 6, 'UTC'), toDateTime64('2023-02-01 12:00:00.000001', 6, 'UTC')], d);

DROP TABLE t_scale_cast_key;
