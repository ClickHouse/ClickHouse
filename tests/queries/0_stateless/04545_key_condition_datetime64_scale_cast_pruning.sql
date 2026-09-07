-- https://github.com/ClickHouse/ClickHouse/issues/117813
-- KeyCondition's direct-CAST fast path (applyDeterministicDagToColumn) skipped the
-- intermediate cast to the key column's own type when the CAST target type already
-- matched, so a DateTime64 constant of a different scale than the sorting/partition
-- key column was rendered to String at its own scale instead of the column's scale.
-- This produced a bogus key range that silently pruned the matching row.

DROP TABLE IF EXISTS t_dt64_scale_pk;
CREATE TABLE t_dt64_scale_pk (d DateTime64(3, 'UTC')) ENGINE = MergeTree ORDER BY d::String;
INSERT INTO t_dt64_scale_pk VALUES (toDateTime64(1675252800, 3, 'UTC'));
SELECT count() FROM t_dt64_scale_pk WHERE d = toDateTime64(1675252800, 6, 'UTC');
DROP TABLE t_dt64_scale_pk;

DROP TABLE IF EXISTS t_dt64_scale_partition;
CREATE TABLE t_dt64_scale_partition (d DateTime64(3, 'UTC')) ENGINE = MergeTree PARTITION BY d::String ORDER BY tuple();
INSERT INTO t_dt64_scale_partition VALUES (toDateTime64(1675252800, 3, 'UTC'));
SELECT count() FROM t_dt64_scale_partition WHERE d = toDateTime64(1675252800, 6, 'UTC');
DROP TABLE t_dt64_scale_partition;

DROP TABLE IF EXISTS t_dt64_scale_reverse;
CREATE TABLE t_dt64_scale_reverse (d DateTime64(6, 'UTC')) ENGINE = MergeTree ORDER BY d::String;
INSERT INTO t_dt64_scale_reverse VALUES (toDateTime64(1675252800, 6, 'UTC'));
SELECT count() FROM t_dt64_scale_reverse WHERE d = toDateTime64(1675252800, 3, 'UTC');
DROP TABLE t_dt64_scale_reverse;

DROP TABLE IF EXISTS t_dt64_scale_multi;
CREATE TABLE t_dt64_scale_multi (d DateTime64(3, 'UTC'), v UInt64) ENGINE = MergeTree ORDER BY d::String;
INSERT INTO t_dt64_scale_multi SELECT toDateTime64(1675252800 + number, 3, 'UTC'), number FROM numbers(10000);
SELECT count() FROM t_dt64_scale_multi WHERE d = toDateTime64(1675262799, 6, 'UTC');
SELECT count() FROM t_dt64_scale_multi WHERE d = toDateTime64(1675262799, 3, 'UTC');
DROP TABLE t_dt64_scale_multi;

-- Controls: pre-existing behavior for matching scale, no CAST, and non-CAST toString() must not regress.

DROP TABLE IF EXISTS t_dt64_scale_control_matching;
CREATE TABLE t_dt64_scale_control_matching (d DateTime64(3, 'UTC')) ENGINE = MergeTree ORDER BY d::String;
INSERT INTO t_dt64_scale_control_matching VALUES (toDateTime64(1675252800, 3, 'UTC'));
SELECT count() FROM t_dt64_scale_control_matching WHERE d = toDateTime64(1675252800, 3, 'UTC');
DROP TABLE t_dt64_scale_control_matching;

DROP TABLE IF EXISTS t_dt64_scale_control_no_cast;
CREATE TABLE t_dt64_scale_control_no_cast (d DateTime64(3, 'UTC')) ENGINE = MergeTree ORDER BY d;
INSERT INTO t_dt64_scale_control_no_cast VALUES (toDateTime64(1675252800, 3, 'UTC'));
SELECT count() FROM t_dt64_scale_control_no_cast WHERE d = toDateTime64(1675252800, 6, 'UTC');
DROP TABLE t_dt64_scale_control_no_cast;

DROP TABLE IF EXISTS t_dt64_scale_control_tostring;
CREATE TABLE t_dt64_scale_control_tostring (d DateTime64(3, 'UTC')) ENGINE = MergeTree ORDER BY toString(d);
INSERT INTO t_dt64_scale_control_tostring VALUES (toDateTime64(1675252800, 3, 'UTC'));
SELECT count() FROM t_dt64_scale_control_tostring WHERE d = toDateTime64(1675252800, 6, 'UTC');
DROP TABLE t_dt64_scale_control_tostring;

-- DateTime constant compared against a DateTime64 key column through the same CAST fast path.
DROP TABLE IF EXISTS t_dt64_vs_dt;
CREATE TABLE t_dt64_vs_dt (d DateTime64(3, 'UTC')) ENGINE = MergeTree ORDER BY d::String;
INSERT INTO t_dt64_vs_dt VALUES (toDateTime64(1675252800, 3, 'UTC'));
SELECT count() FROM t_dt64_vs_dt WHERE d = toDateTime(1675252800, 'UTC');
DROP TABLE t_dt64_vs_dt;

-- Date constant compared against a DateTime64 key column through the same CAST fast path.
DROP TABLE IF EXISTS t_dt64_vs_date;
CREATE TABLE t_dt64_vs_date (d DateTime64(3, 'UTC')) ENGINE = MergeTree ORDER BY d::String;
INSERT INTO t_dt64_vs_date VALUES (toDateTime64(1675209600, 3, 'UTC'));
SELECT count() FROM t_dt64_vs_date WHERE d = toDate('2023-02-01');
DROP TABLE t_dt64_vs_date;

-- Same scale-mismatch mechanism also affects Time64.
DROP TABLE IF EXISTS t_time64_scale;
CREATE TABLE t_time64_scale (d Time64(3)) ENGINE = MergeTree ORDER BY d::String;
INSERT INTO t_time64_scale VALUES (toTime64('12:00:00', 3));
SELECT count() FROM t_time64_scale WHERE d = toTime64('12:00:00', 6);
DROP TABLE t_time64_scale;

-- Control: the String -> Dynamic -> String round-trip this fast path exists for must still work.
DROP TABLE IF EXISTS t_dynamic_control;
CREATE TABLE t_dynamic_control (d Dynamic) ENGINE = MergeTree ORDER BY d::String;
INSERT INTO t_dynamic_control VALUES ('hello');
SELECT count() FROM t_dynamic_control WHERE d::String = 'hello';
DROP TABLE t_dynamic_control;
