SET mutations_sync=2;

DROP TABLE IF EXISTS t_ephemeral_02205_1;

CREATE TABLE t_ephemeral_02205_1 (x UInt32 DEFAULT y, y UInt32 EPHEMERAL 17, z UInt32 DEFAULT 5) ENGINE = Memory;

DESCRIBE t_ephemeral_02205_1;

# Test INSERT without columns list - should participate only ordinary columns (x, z)
INSERT INTO t_ephemeral_02205_1 VALUES (1, 2);
# SELECT * should only return ordinary columns (x, z) - ephemeral is not stored in the table
SELECT * FROM t_ephemeral_02205_1;

TRUNCATE TABLE t_ephemeral_02205_1;

INSERT INTO t_ephemeral_02205_1 VALUES (DEFAULT, 2);
SELECT * FROM t_ephemeral_02205_1;

TRUNCATE TABLE t_ephemeral_02205_1;

# Test INSERT using ephemerals default
INSERT INTO t_ephemeral_02205_1 (x, y) VALUES (DEFAULT, DEFAULT);
SELECT * FROM t_ephemeral_02205_1;

TRUNCATE TABLE t_ephemeral_02205_1;

# Test INSERT using explicit ephemerals value
INSERT INTO t_ephemeral_02205_1 (x, y) VALUES (DEFAULT, 7);
SELECT * FROM t_ephemeral_02205_1;

# Test ALTER TABLE DELETE
ALTER TABLE t_ephemeral_02205_1 DELETE WHERE x = 7;
SELECT * FROM t_ephemeral_02205_1;

TRUNCATE TABLE t_ephemeral_02205_1;

# Test INSERT into column, defaulted to ephemeral, but explicitly provided with value
INSERT INTO t_ephemeral_02205_1 (x, y) VALUES (21, 7);
SELECT * FROM t_ephemeral_02205_1;


DROP TABLE IF EXISTS t_ephemeral_02205_1;

# Test without default
CREATE TABLE t_ephemeral_02205_1 (x UInt32 DEFAULT y, y UInt32 EPHEMERAL, z UInt32 DEFAULT 5) ENGINE = Memory;

DESCRIBE t_ephemeral_02205_1;

# Test INSERT without columns list - should participate only ordinary columns (x, z)
INSERT INTO t_ephemeral_02205_1 VALUES (1, 2);
# SELECT * should only return ordinary columns (x, z) - ephemeral is not stored in the table
SELECT * FROM t_ephemeral_02205_1;

TRUNCATE TABLE t_ephemeral_02205_1;

INSERT INTO t_ephemeral_02205_1 VALUES (DEFAULT, 2);
SELECT * FROM t_ephemeral_02205_1;

TRUNCATE TABLE t_ephemeral_02205_1;

# Test INSERT using ephemerals default
INSERT INTO t_ephemeral_02205_1 (x, y) VALUES (DEFAULT, DEFAULT);
SELECT * FROM t_ephemeral_02205_1;

TRUNCATE TABLE t_ephemeral_02205_1;

# Test INSERT using explicit ephemerals value
INSERT INTO t_ephemeral_02205_1 (x, y) VALUES (DEFAULT, 7);
SELECT * FROM t_ephemeral_02205_1;

# Test ALTER TABLE DELETE
ALTER TABLE t_ephemeral_02205_1 DELETE WHERE x = 7;
SELECT * FROM t_ephemeral_02205_1;

TRUNCATE TABLE t_ephemeral_02205_1;

# Test INSERT into column, defaulted to ephemeral, but explicitly provided with value
INSERT INTO t_ephemeral_02205_1 (x, y) VALUES (21, 7);
SELECT * FROM t_ephemeral_02205_1;

DROP TABLE IF EXISTS t_ephemeral_02205_1;

