DROP TABLE IF EXISTS arrays_test;
CREATE TABLE arrays_test (s String, arr Array(UInt8)) ENGINE = Memory;
INSERT INTO arrays_test VALUES ('Hello', [1,2]), ('World', [3,4,5]), ('Goodbye', []);
