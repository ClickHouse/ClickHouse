-- Partition-targeting ALTER on a UUID2 partition key must resolve the right partition even when the
-- constant is typed as UUID (the layouts differ by swapping the 64-bit halves, so dropping the source
-- type of the constant would compute a wrong partition ID and silently target a non-existent partition).

DROP TABLE IF EXISTS t_uuid2_partition;
CREATE TABLE t_uuid2_partition (id UUID2) ENGINE = MergeTree PARTITION BY id ORDER BY id;
INSERT INTO t_uuid2_partition VALUES ('61f0c404-5cb3-11e7-907b-a6006ad3dba0'), ('992f6910-42b2-43cd-98bc-c812fbf9b683');
SELECT count() FROM t_uuid2_partition;

-- A typed UUID constant: the value arrives in the historical UUID layout and must be swapped for the UUID2 key.
ALTER TABLE t_uuid2_partition DROP PARTITION tuple(toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0'));
SELECT count() FROM t_uuid2_partition;

-- A typed UUID constant via a cast, and a typed UUID2 constant (already in the needed layout).
ALTER TABLE t_uuid2_partition DROP PARTITION '992f6910-42b2-43cd-98bc-c812fbf9b683'::UUID;
SELECT count() FROM t_uuid2_partition;

DROP TABLE t_uuid2_partition;

-- The same for a complex (tuple) partition key.
DROP TABLE IF EXISTS t_uuid2_partition_tuple;
CREATE TABLE t_uuid2_partition_tuple (id UUID2, n UInt8) ENGINE = MergeTree PARTITION BY (id, n) ORDER BY id;
INSERT INTO t_uuid2_partition_tuple VALUES ('61f0c404-5cb3-11e7-907b-a6006ad3dba0', 1), ('992f6910-42b2-43cd-98bc-c812fbf9b683', 2);
SELECT count() FROM t_uuid2_partition_tuple;

ALTER TABLE t_uuid2_partition_tuple DROP PARTITION (toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0'), 1);
SELECT count() FROM t_uuid2_partition_tuple;

ALTER TABLE t_uuid2_partition_tuple DROP PARTITION (toUUID2('992f6910-42b2-43cd-98bc-c812fbf9b683'), 2);
SELECT count() FROM t_uuid2_partition_tuple;

DROP TABLE t_uuid2_partition_tuple;

-- A plain string literal is parsed with the destination type's text format and needs no swap,
-- and a typed UUID2 cast is already in the needed layout.
DROP TABLE IF EXISTS t_uuid2_partition_str;
CREATE TABLE t_uuid2_partition_str (id UUID2) ENGINE = MergeTree PARTITION BY id ORDER BY id;
INSERT INTO t_uuid2_partition_str VALUES ('61f0c404-5cb3-11e7-907b-a6006ad3dba0'), ('992f6910-42b2-43cd-98bc-c812fbf9b683');
ALTER TABLE t_uuid2_partition_str DROP PARTITION '61f0c404-5cb3-11e7-907b-a6006ad3dba0';
ALTER TABLE t_uuid2_partition_str DROP PARTITION '992f6910-42b2-43cd-98bc-c812fbf9b683'::UUID2;
SELECT count() FROM t_uuid2_partition_str;
DROP TABLE t_uuid2_partition_str;
