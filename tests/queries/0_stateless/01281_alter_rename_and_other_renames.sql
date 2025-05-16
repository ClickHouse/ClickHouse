DROP TABLE IF EXISTS rename_table_multiple;

CREATE TABLE rename_table_multiple (key Int32, value1 String, value2 Int32) ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part=0;

INSERT INTO rename_table_multiple VALUES (1, 2, 3);

ALTER TABLE rename_table_multiple RENAME COLUMN value1 TO value1_string, MODIFY COLUMN value1_string String; --{serverError NOT_IMPLEMENTED}
ALTER TABLE rename_table_multiple MODIFY COLUMN value1 String, RENAME COLUMN value1 to value1_string; --{serverError NOT_IMPLEMENTED}

ALTER TABLE rename_table_multiple RENAME COLUMN value1 TO value1_string;
ALTER TABLE rename_table_multiple MODIFY COLUMN value1_string String;

SHOW CREATE TABLE rename_table_multiple;

SELECT * FROM rename_table_multiple FORMAT TSVWithNames;

INSERT INTO rename_table_multiple VALUES (4, '5', 6);

ALTER TABLE rename_table_multiple RENAME COLUMN value2 TO value2_old, ADD COLUMN value2 Int64 DEFAULT 7;

SHOW CREATE TABLE rename_table_multiple;

SELECT * FROM rename_table_multiple ORDER BY key FORMAT TSVWithNames;

INSERT INTO rename_table_multiple VALUES (7, '8', 9, 10);

ALTER TABLE rename_table_multiple DROP COLUMN value2_old, RENAME COLUMN value2 TO value2_old;

SHOW CREATE TABLE rename_table_multiple;

SELECT * FROM rename_table_multiple ORDER BY key FORMAT TSVWithNames;

DROP TABLE IF EXISTS rename_table_multiple;

DROP TABLE IF EXISTS rename_table_multiple_compact;

CREATE TABLE rename_table_multiple_compact (key Int32, value1 String, value2 Int32) ENGINE = MergeTree ORDER BY tuple() SETTINGS min_rows_for_wide_part = 100000;

INSERT INTO rename_table_multiple_compact VALUES (1, 2, 3);

ALTER TABLE rename_table_multiple_compact RENAME COLUMN value1 TO value1_string, MODIFY COLUMN value1_string String; --{serverError NOT_IMPLEMENTED}
ALTER TABLE rename_table_multiple_compact MODIFY COLUMN value1 String, RENAME COLUMN value1 to value1_string; --{serverError NOT_IMPLEMENTED}

ALTER TABLE rename_table_multiple_compact RENAME COLUMN value1 TO value1_string;
ALTER TABLE rename_table_multiple_compact MODIFY COLUMN value1_string String;

SHOW CREATE TABLE rename_table_multiple_compact;

SELECT * FROM rename_table_multiple_compact FORMAT TSVWithNames;

INSERT INTO rename_table_multiple_compact VALUES (4, '5', 6);

ALTER TABLE rename_table_multiple_compact RENAME COLUMN value2 TO value2_old, ADD COLUMN value2 Int64 DEFAULT 7;

SHOW CREATE TABLE rename_table_multiple_compact;

SELECT * FROM rename_table_multiple_compact ORDER BY key FORMAT TSVWithNames;

INSERT INTO rename_table_multiple_compact VALUES (7, '8', 9, 10);

ALTER TABLE rename_table_multiple_compact DROP COLUMN value2_old, RENAME COLUMN value2 TO value2_old;

SHOW CREATE TABLE rename_table_multiple_compact;

SELECT * FROM rename_table_multiple_compact ORDER BY key FORMAT TSVWithNames;

DROP TABLE IF EXISTS rename_table_multiple_compact;
