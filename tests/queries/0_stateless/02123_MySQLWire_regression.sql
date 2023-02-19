DROP TABLE IF EXISTS table_MySQLWire;
CREATE TABLE table_MySQLWire (x UInt64) ENGINE = File(MySQLWire);
INSERT INTO table_MySQLWire SELECT number FROM numbers(10);
-- regression for not initializing serializations
INSERT INTO table_MySQLWire SELECT number FROM numbers(10);
DROP TABLE table_MySQLWire;
