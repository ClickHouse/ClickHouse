-- An extended-range `Date32` constant used as an exact bound for a `DateTime64` (or `Time64`) column with
-- a large scale can produce a whole-seconds value that does not fit into the underlying `Int64` after
-- multiplying by the scale multiplier. `convertFieldToType` must treat such a bound as "cannot convert"
-- (it cannot equal any stored value) instead of throwing `DECIMAL_OVERFLOW`.

DROP TABLE IF EXISTS t_d64;
CREATE TABLE t_d64 (d64 DateTime64(9, 'UTC')) ENGINE = MergeTree ORDER BY d64;
INSERT INTO t_d64 VALUES ('2020-01-01 00:00:00');

SELECT count() FROM t_d64 WHERE d64 IN (toDate32('9999-12-31'));
SELECT count() FROM t_d64 WHERE d64 IN (toDate32('0000-01-01'));
SELECT count() FROM t_d64 WHERE d64 IN (toDate32('2020-01-01'));
SELECT count() FROM t_d64 WHERE d64 = toDate32('9999-12-31');

DROP TABLE t_d64;

-- A smaller scale still represents the extended range exactly and must keep matching.
DROP TABLE IF EXISTS t_d64_small;
CREATE TABLE t_d64_small (d64 DateTime64(3, 'UTC')) ENGINE = MergeTree ORDER BY d64;
INSERT INTO t_d64_small VALUES ('9999-12-31 00:00:00'), ('2020-01-01 00:00:00');

SELECT count() FROM t_d64_small WHERE d64 IN (toDate32('9999-12-31'));
SELECT count() FROM t_d64_small WHERE d64 IN (toDate32('0000-01-01'));

DROP TABLE t_d64_small;

DROP TABLE IF EXISTS t_t64;
CREATE TABLE t_t64 (t64 Time64(9)) ENGINE = MergeTree ORDER BY t64;
INSERT INTO t_t64 VALUES ('00:00:00');

SELECT count() FROM t_t64 WHERE t64 IN (toDate32('9999-12-31'));

DROP TABLE t_t64;
