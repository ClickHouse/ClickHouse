DROP TABLE IF EXISTS table_with_defaults_on_aliases;

CREATE TABLE table_with_defaults_on_aliases (col1 UInt32, col2 ALIAS col1, col3 DEFAULT col2) Engine = MergeTree() ORDER BY tuple();

SYSTEM STOP MERGES table_with_defaults_on_aliases;

INSERT INTO table_with_defaults_on_aliases (col1) VALUES (1);

SELECT * FROM table_with_defaults_on_aliases WHERE col1 = 1;

SELECT col1, col2, col3 FROM table_with_defaults_on_aliases WHERE col1 = 1;

SELECT col3 FROM table_with_defaults_on_aliases; -- important to check without WHERE

ALTER TABLE table_with_defaults_on_aliases ADD COLUMN col4 UInt64 DEFAULT col2 * col3;

INSERT INTO table_with_defaults_on_aliases (col1) VALUES (2);

SELECT * FROM table_with_defaults_on_aliases WHERE col1 = 2;

SELECT col1, col2, col3, col4 FROM table_with_defaults_on_aliases WHERE col1 = 2;

ALTER TABLE table_with_defaults_on_aliases ADD COLUMN col5 UInt64 ALIAS col2 * col4;

INSERT INTO table_with_defaults_on_aliases (col1) VALUES (3);

SELECT * FROM table_with_defaults_on_aliases WHERE col1 = 3;

SELECT col1, col2, col3, col4, col5 FROM table_with_defaults_on_aliases WHERE col1 = 3;

ALTER TABLE table_with_defaults_on_aliases ADD COLUMN col6 UInt64 MATERIALIZED col2 * col4;

DROP TABLE IF EXISTS table_with_defaults_on_aliases;
