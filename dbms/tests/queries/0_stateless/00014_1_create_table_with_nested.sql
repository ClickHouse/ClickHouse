DROP TABLE IF EXISTS nested_test;
CREATE TABLE nested_test (s String, nest Nested(x UInt8, y UInt32)) ENGINE = Memory;
INSERT INTO nested_test VALUES ('Hello', [1,2], [10,20]), ('World', [3,4,5], [30,40,50]), ('Goodbye', [], []);
