-- `_partition_value` exposes the values produced by the adjusted partition key (`modulo` is computed
-- as `moduloLegacy`); an element takes that key's type when the declared type cannot represent it.

-- The produced value is negative and the declared type is unsigned, so it is reinterpreted.
CREATE TABLE mod_wrong (c0 Int32) ENGINE = MergeTree ORDER BY tuple() PARTITION BY (3000000000 % c0);
INSERT INTO mod_wrong VALUES (-1);
SELECT 'wrong-value', toTypeName(_partition_value), _partition_value FROM mod_wrong;
SELECT 'wrong-value agrees with the part', partition_id FROM system.parts
WHERE database = currentDatabase() AND table = 'mod_wrong' AND active;

CREATE TABLE mod_value (c0 Int128) ENGINE = MergeTree ORDER BY tuple()
PARTITION BY (CAST(37528, 'UInt64') % c0);
INSERT INTO mod_value VALUES (167682982);
SELECT 'reader', toTypeName(_partition_value), _partition_value FROM mod_value;
SELECT 'filter', count() FROM mod_value WHERE _partition_value.1 = 37528;

-- A produced value that the declared type cannot represent at all.
CREATE TABLE mod_negative (c0 Int128) ENGINE = MergeTree ORDER BY tuple()
PARTITION BY (CAST(-1, 'UInt128') % c0);
INSERT INTO mod_negative VALUES (1000);
SELECT 'negative', _partition_value FROM mod_negative;

-- Reading through a normal projection resolves the column against the projection's own metadata.
CREATE TABLE mod_projection (a Int128, b UInt32, PROJECTION p (SELECT a, b ORDER BY b))
ENGINE = MergeTree PARTITION BY (CAST(37528, 'UInt64') % a) ORDER BY a SETTINGS index_granularity = 1;
INSERT INTO mod_projection VALUES (167682982, 10);
SELECT 'projection', b, _partition_value FROM mod_projection WHERE b = 10
SETTINGS force_optimize_projection = 1, optimize_use_projections = 1;

-- A declared type that is merely wider than the produced one keeps its declared type.
CREATE TABLE mod_widening (c0 Int32) ENGINE = MergeTree ORDER BY tuple() PARTITION BY (c0 % 100);
INSERT INTO mod_widening VALUES (12345);
SELECT 'widening', toTypeName(_partition_value), _partition_value FROM mod_widening;

-- Below 128 bits the signedness divergence never aborted, but the declared type still mislabelled it.
CREATE TABLE mod_narrow (c0 Int32) ENGINE = MergeTree ORDER BY tuple()
PARTITION BY (CAST(37528, 'UInt32') % c0);
INSERT INTO mod_narrow VALUES (1000);
SELECT 'narrow', toTypeName(_partition_value), _partition_value FROM mod_narrow;

-- The choice is per element: here the first element diverges and the second merely widens, so only
-- the first takes the produced type.
CREATE TABLE mod_mixed (c0 Int128, c1 Int32) ENGINE = MergeTree ORDER BY tuple()
PARTITION BY (CAST(37528, 'UInt64') % c0, c1 % 100);
INSERT INTO mod_mixed VALUES (167682982, 12345);
SELECT 'mixed', toTypeName(_partition_value), _partition_value FROM mod_mixed;
