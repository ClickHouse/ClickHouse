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
