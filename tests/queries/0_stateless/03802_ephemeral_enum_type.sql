-- Test for parsing EPHEMERAL columns with Enum types (no default expression)
-- This used to cause undefined behavior due to null pointer dereference in parser

DROP TABLE IF EXISTS t_ephemeral_enum;

CREATE TABLE t_ephemeral_enum (x UInt32, y Enum8('a' = 1, 'b' = 2) EPHEMERAL) ENGINE = Memory;
DESCRIBE TABLE t_ephemeral_enum FORMAT TSVRaw;
DROP TABLE t_ephemeral_enum;

CREATE TABLE t_ephemeral_enum (x UInt32, y Enum16('foo' = 100, 'bar' = 200) EPHEMERAL) ENGINE = Memory;
DESCRIBE TABLE t_ephemeral_enum FORMAT TSVRaw;
DROP TABLE t_ephemeral_enum;
