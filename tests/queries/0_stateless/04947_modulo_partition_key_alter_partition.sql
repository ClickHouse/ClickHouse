-- Partition manipulation for a key containing `modulo` whose left operand is unsigned and whose right
-- operand is signed: the value computed for a part and the value parsed from the query must agree.

CREATE TABLE mod_drop (c0 Int32) ENGINE = MergeTree ORDER BY tuple() PARTITION BY (37528 % c0);
INSERT INTO mod_drop VALUES (167682982);
ALTER TABLE mod_drop DROP PARTITION 37528;
SELECT 'drop partition', count() FROM mod_drop;

SET mutations_sync = 2;
CREATE TABLE mod_delete (c0 Int32) ENGINE = MergeTree ORDER BY tuple() PARTITION BY (37528 % c0);
INSERT INTO mod_delete VALUES (167682982);
DELETE FROM mod_delete IN PARTITION 37528 WHERE 1;
SELECT 'delete in partition', count() FROM mod_delete;

CREATE TABLE mod_complex (c0 Int32) ENGINE = MergeTree ORDER BY tuple() PARTITION BY (c0, 37528 % c0);
INSERT INTO mod_complex VALUES (167682982);
ALTER TABLE mod_complex DROP PARTITION (167682982, 37528);
SELECT 'complex key', count() FROM mod_complex;

-- The partition value is hashed into the partition ID once it no longer fits 8 bytes.
CREATE TABLE mod_wide (c0 Int128) ENGINE = MergeTree ORDER BY tuple() PARTITION BY (CAST(37528, 'UInt64') % c0);
INSERT INTO mod_wide VALUES (167682982);
ALTER TABLE mod_wide DROP PARTITION 37528;
SELECT 'wide key', count() FROM mod_wide;
DROP TABLE mod_wide;

-- A value outside the range of the partition key type addresses no partition and is rejected.
CREATE TABLE mod_range (c0 Int32) ENGINE = MergeTree ORDER BY tuple() PARTITION BY (c0 % 37528);
INSERT INTO mod_range VALUES (5);
ALTER TABLE mod_range DROP PARTITION 40000; -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT 'out of range', count() FROM mod_range;

-- Partition IDs must be unaffected.
CREATE TABLE mod_id (d Date, c0 Int32) ENGINE = MergeTree ORDER BY tuple() PARTITION BY (d, 37528 % c0);
INSERT INTO mod_id VALUES ('2020-05-23', 167682982);
SELECT 'partition id', partition_id FROM system.parts WHERE database = currentDatabase() AND table = 'mod_id' AND active;

-- Keys where the result signedness is the same either way, and keys with no `modulo` at all.
CREATE TABLE mod_unsigned (c0 UInt32) ENGINE = MergeTree ORDER BY tuple() PARTITION BY (37528 % c0);
INSERT INTO mod_unsigned VALUES (167682982);
ALTER TABLE mod_unsigned DROP PARTITION 37528;
SELECT 'unsigned column', count() FROM mod_unsigned;

CREATE TABLE mod_signed_left (c0 Int32) ENGINE = MergeTree ORDER BY tuple() PARTITION BY (c0 % 37528);
INSERT INTO mod_signed_left VALUES (5);
ALTER TABLE mod_signed_left DROP PARTITION 5;
SELECT 'signed left operand', count() FROM mod_signed_left;

CREATE TABLE mod_none (c0 Int32) ENGINE = MergeTree ORDER BY tuple() PARTITION BY positiveModulo(37528, c0);
INSERT INTO mod_none VALUES (167682982);
ALTER TABLE mod_none DROP PARTITION 37528;
SELECT 'no modulo', count() FROM mod_none;
