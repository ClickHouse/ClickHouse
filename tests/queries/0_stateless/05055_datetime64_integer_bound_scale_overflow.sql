-- An integer constant used as an exact bound for a `DateTime64` column is a number of seconds, and scaling it
-- up to the tick count of a large-scale column can overflow the underlying `Int64`. `convertFieldToType` must
-- treat such a bound as "cannot convert" (it cannot equal any stored value) instead of throwing
-- `DECIMAL_OVERFLOW` out of the comparison, like it already does for a `Date32` bound.

DROP TABLE IF EXISTS t_d64_nanoseconds;
CREATE TABLE t_d64_nanoseconds (d64 DateTime64(9, 'UTC')) ENGINE = MergeTree ORDER BY d64;
INSERT INTO t_d64_nanoseconds VALUES ('2020-01-01 00:00:00');

SELECT count() FROM t_d64_nanoseconds WHERE d64 IN (253402300799);
SELECT count() FROM t_d64_nanoseconds WHERE d64 IN (1577836800);

DROP TABLE t_d64_nanoseconds;

-- A tick count outside the calendar window `[0000-01-01, 9999-12-31]` still fits the `Int64` storage of a
-- second-precision column, and such a value can really be stored, so it must keep matching its own row
-- even though it is displayed clamped to the boundary.

DROP TABLE IF EXISTS t_d64_seconds;
CREATE TABLE t_d64_seconds (d64 DateTime64(0, 'UTC')) ENGINE = MergeTree ORDER BY d64;
INSERT INTO t_d64_seconds VALUES (253402300799), (253402300800);

SELECT d64 FROM t_d64_seconds ORDER BY d64;
SELECT count() FROM t_d64_seconds WHERE d64 IN (253402300800);
SELECT count() FROM t_d64_seconds WHERE d64 IN (253402300799);

DROP TABLE t_d64_seconds;
