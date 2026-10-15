-- Tests that _sample_factor is materialized by the per-part read path. See issue #78465.

DROP TABLE IF EXISTS t_mut_sf;
CREATE TABLE t_mut_sf (c0 UInt32, u Float64) ENGINE = MergeTree ORDER BY c0 SAMPLE BY c0;
INSERT INTO t_mut_sf VALUES (1, 0), (5, 0);

SELECT 'select', _sample_factor FROM t_mut_sf ORDER BY c0 LIMIT 1;

SET mutations_sync = 2;

ALTER TABLE t_mut_sf UPDATE u = _sample_factor WHERE c0 < 2;
SELECT 'update rhs', u FROM t_mut_sf ORDER BY c0;

ALTER TABLE t_mut_sf UPDATE u = 7 WHERE _sample_factor = 1 AND c0 > 4;
SELECT 'update where', u FROM t_mut_sf ORDER BY c0;

ALTER TABLE t_mut_sf DELETE WHERE _sample_factor = 1 AND c0 < 2;
SELECT 'alter delete', count() FROM t_mut_sf;

DELETE FROM t_mut_sf WHERE _sample_factor = 1 AND c0 > 4;
SELECT 'lightweight delete', count() FROM t_mut_sf;

DROP TABLE t_mut_sf;

-- A real Tuple subcolumn sharing a virtual column name stays a plain column access.
DROP TABLE IF EXISTS t_mut_subcol;
CREATE TABLE t_mut_subcol (c0 UInt32, tup Tuple(_sample_factor Float64, _table UInt8)) ENGINE = MergeTree ORDER BY c0;
INSERT INTO t_mut_subcol VALUES (1, (0.5, 7)), (5, (0.5, 7));
ALTER TABLE t_mut_subcol DELETE WHERE tup._sample_factor = 0.5 AND c0 < 2;
SELECT 'tuple subcolumn', count() FROM t_mut_subcol;
DROP TABLE t_mut_subcol;

-- A lambda parameter only shares the name with the virtual column.
DROP TABLE IF EXISTS t_mut_shadow;
CREATE TABLE t_mut_shadow (c0 UInt32, arr Array(Float64)) ENGINE = MergeTree ORDER BY c0;
INSERT INTO t_mut_shadow VALUES (1, [1]), (5, [1]);
ALTER TABLE t_mut_shadow DELETE WHERE arrayExists(_sample_factor -> _sample_factor = 1, arr) AND c0 < 2;
SELECT 'lambda parameter', count() FROM t_mut_shadow;
DROP TABLE t_mut_shadow;

-- _table and _database come from the local storage id, which a replica cannot reproduce, so
-- a mutation still cannot materialize them and fails while the part is read.
DROP TABLE IF EXISTS t_mut_local;
CREATE TABLE t_mut_local (c0 UInt32) ENGINE = MergeTree ORDER BY c0;
INSERT INTO t_mut_local VALUES (1), (5);
ALTER TABLE t_mut_local DELETE WHERE _table != '' AND c0 < 2; -- { serverError UNFINISHED }
SELECT 'local storage id', count() FROM t_mut_local;
DROP TABLE t_mut_local;

-- A real column named like a virtual one is a separate pre-existing failure. It is raised
-- by the analyzer during mutation validation, so that statement pins both settings.
DROP TABLE IF EXISTS t_mut_override;
CREATE TABLE t_mut_override (c0 UInt32, _sample_factor Float64) ENGINE = MergeTree ORDER BY c0;
INSERT INTO t_mut_override VALUES (1, 0.5), (5, 0.5);
ALTER TABLE t_mut_override DELETE WHERE _sample_factor = 0.5 AND c0 < 2 SETTINGS enable_analyzer = 1, validate_mutation_query = 1; -- { serverError NOT_FOUND_COLUMN_IN_BLOCK }
SELECT 'real column override', count() FROM t_mut_override;
DROP TABLE t_mut_override;
