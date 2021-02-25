DROP TABLE IF EXISTS nested_3;
CREATE TABLE nested_3 (n Nested(a UInt32, s String)) ENGINE = Log;

INSERT INTO nested_3 VALUES ([1, 2], ['a', 'b']);

SELECT 1 FROM nested_3;
SELECT count() FROM nested_3;

DROP TABLE nested_3;
