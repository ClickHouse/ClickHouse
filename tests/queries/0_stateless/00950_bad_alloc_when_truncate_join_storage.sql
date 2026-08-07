DROP TABLE IF EXISTS join_test;
CREATE TABLE join_test (number UInt8, value Float32) Engine = Join(ANY, LEFT, number);
TRUNCATE TABLE join_test;
DROP TABLE IF EXISTS join_test;
