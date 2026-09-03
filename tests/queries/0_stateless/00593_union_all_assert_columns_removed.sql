DROP TABLE IF EXISTS columns;
CREATE TABLE columns (a UInt8, b UInt8, c UInt8) ENGINE = Memory;
INSERT INTO columns VALUES (1, 2, 3);
SET max_columns_to_read = 1;

SELECT a FROM (SELECT * FROM columns);
SELECT a FROM (SELECT * FROM (SELECT * FROM columns));
SELECT a FROM (SELECT * FROM columns UNION ALL SELECT * FROM columns);

DROP TABLE columns;
