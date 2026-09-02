-- https://github.com/ClickHouse/ClickHouse/issues/113854
-- A table-backed set converts the probe column into the set's key type. A value the key type cannot
-- represent is simply not a member - which is what the literal `IN` list concludes - but the strict
-- conversion raised `CANNOT_CONVERT_TYPE` on the rows the read path happened to deliver, so the same
-- query succeeded or failed depending on how much the plan pruned.

DROP TABLE IF EXISTS t_set_engine;
DROP TABLE IF EXISTS t_set_probe;
CREATE TABLE t_set_engine (k UInt64) ENGINE = Set;
INSERT INTO t_set_engine VALUES (1);
CREATE TABLE t_set_probe (v Int64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_set_probe VALUES (1), (-1);

SELECT count() FROM t_set_probe WHERE v IN (1);
SELECT count() FROM t_set_probe WHERE v IN t_set_engine;
SELECT v FROM t_set_probe WHERE v IN t_set_engine ORDER BY v;
SELECT v, v IN t_set_engine FROM t_set_probe ORDER BY v;
SELECT count() FROM t_set_probe WHERE v NOT IN t_set_engine;
SELECT count() FROM t_set_probe WHERE v IN t_set_engine SETTINGS use_skip_indexes = 0, optimize_use_implicit_projections = 0;

SELECT 'a wider probe type';
DROP TABLE IF EXISTS t_set_probe_wide;
CREATE TABLE t_set_probe_wide (v Int128) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_set_probe_wide VALUES (1), (-1), (18446744073709551616);
SELECT count() FROM t_set_probe_wide WHERE v IN t_set_engine;
SELECT v FROM t_set_probe_wide WHERE v IN t_set_engine ORDER BY v;
DROP TABLE t_set_probe_wide;

SELECT 'a Nullable set key: the cross-type probe agrees with the same-type one';
DROP TABLE IF EXISTS t_set_nullable;
DROP TABLE IF EXISTS t_probe_same_type;
DROP TABLE IF EXISTS t_probe_nullable;
CREATE TABLE t_set_nullable (k Nullable(UInt64)) ENGINE = Set;
INSERT INTO t_set_nullable VALUES (1), (NULL);
CREATE TABLE t_probe_same_type (v Nullable(UInt64)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_probe_same_type VALUES (1), (2), (NULL);
CREATE TABLE t_probe_nullable (v Nullable(Int64)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_probe_nullable VALUES (1), (-1), (NULL);
SELECT v, v IN t_set_nullable FROM t_probe_same_type ORDER BY v NULLS LAST;
SELECT v, v IN t_set_nullable FROM t_probe_nullable ORDER BY v NULLS LAST;
SELECT count() FROM t_probe_nullable WHERE v IN t_set_nullable;
DROP TABLE t_set_nullable;
DROP TABLE t_probe_same_type;
DROP TABLE t_probe_nullable;

SELECT 'a non-nullable set key with a nullable probe';
DROP TABLE IF EXISTS t_probe_nullable2;
CREATE TABLE t_probe_nullable2 (v Nullable(Int64)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_probe_nullable2 VALUES (1), (-1), (NULL);
SELECT count() FROM t_probe_nullable2 WHERE v IN t_set_engine;
SELECT count() FROM t_probe_nullable2 WHERE v IN (1);
DROP TABLE t_probe_nullable2;

DROP TABLE t_set_engine;
DROP TABLE t_set_probe;
