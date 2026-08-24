CREATE TEMPORARY TABLE IF NOT EXISTS default_table (x UInt32, y UInt32 DEFAULT 42, z UInt32 DEFAULT 33) ENGINE = Memory;

INSERT INTO default_table(x) values (DEFAULT);
INSERT INTO default_table(x, z) values (1, DEFAULT);
INSERT INTO default_table values (2, 33, DEFAULT);

SELECT * FROM default_table ORDER BY x;
