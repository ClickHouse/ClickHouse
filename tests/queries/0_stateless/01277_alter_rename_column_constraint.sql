DROP TABLE IF EXISTS table_for_rename;

CREATE TABLE table_for_rename
(
  date Date,
  key UInt64,
  value1 String,
  value2 String,
  value3 String,
  CONSTRAINT cs_value1 CHECK toInt64(value1) < toInt64(value2),
  CONSTRAINT cs_value2 CHECK toInt64(value2) < toInt64(value3)
)
ENGINE = MergeTree()
PARTITION BY date
ORDER BY key;

INSERT INTO table_for_rename SELECT toDate('2019-10-01') + number % 3, number, toString(number), toString(number + 1), toString(number + 2) from numbers(9);
INSERT INTO table_for_rename SELECT toDate('2019-10-01') + number % 3, number, toString(number), toString(number + 1), toString(number) from numbers(9); --{serverError 469}

SELECT * FROM table_for_rename ORDER BY key;

ALTER TABLE table_for_rename RENAME COLUMN value1 TO value4;
ALTER TABLE table_for_rename RENAME COLUMN value2 TO value5;
SHOW CREATE TABLE table_for_rename;
SELECT * FROM table_for_rename ORDER BY key;

SELECT '-- insert after rename --';
INSERT INTO table_for_rename SELECT toDate('2019-10-01') + number % 3, number, toString(number), toString(number + 1), toString(number + 2) from numbers(10, 10);
INSERT INTO table_for_rename SELECT toDate('2019-10-01') + number % 3, number, toString(number), toString(number + 1), toString(number) from numbers(10, 10); --{serverError 469}
SELECT * FROM table_for_rename ORDER BY key;

SELECT '-- rename columns back --';
ALTER TABLE table_for_rename RENAME COLUMN value4 TO value1;
ALTER TABLE table_for_rename RENAME COLUMN value5 TO value2;
SHOW CREATE TABLE table_for_rename;
SELECT * FROM table_for_rename ORDER BY key;

SELECT '-- insert after rename column --';
INSERT INTO table_for_rename SELECT toDate('2019-10-01') + number % 3, number, toString(number), toString(number + 1), toString(number + 2) from numbers(20,10);
INSERT INTO table_for_rename SELECT toDate('2019-10-01') + number % 3, number, toString(number), toString(number), toString(number + 2) from numbers(20, 10); --{serverError 469}
SELECT * FROM table_for_rename ORDER BY key;

DROP TABLE IF EXISTS table_for_rename;
