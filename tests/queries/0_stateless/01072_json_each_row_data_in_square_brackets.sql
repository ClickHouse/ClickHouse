-- Tags: no-fasttest

DROP TABLE IF EXISTS json_square_brackets;
CREATE TABLE json_square_brackets (id UInt32, name String) ENGINE = Memory;

INSERT INTO json_square_brackets FORMAT JSONEachRow [{"id": 1, "name": "name1"}, {"id": 2, "name": "name2"}];

INSERT INTO json_square_brackets FORMAT JSONEachRow[];

INSERT INTO json_square_brackets FORMAT JSONEachRow [  ]  ;

INSERT INTO json_square_brackets FORMAT JSONEachRow ;

SELECT * FROM json_square_brackets ORDER BY id;
DROP TABLE IF EXISTS json_square_brackets;
