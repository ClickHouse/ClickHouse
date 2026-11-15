DROP TABLE IF EXISTS table_with_lc_key;

CREATE TABLE table_with_lc_key
(
    enum_key Enum8('x' = 2, 'y' = 1),
    lc_key LowCardinality(String),
    value String
)
ENGINE MergeTree()
ORDER BY (enum_key, lc_key);

INSERT INTO table_with_lc_key VALUES(1, 'hello', 'world');

ALTER TABLE table_with_lc_key MODIFY COLUMN lc_key String;

SHOW CREATE TABLE table_with_lc_key;

DETACH TABLE table_with_lc_key;
ATTACH TABLE table_with_lc_key;

SELECT * FROM table_with_lc_key WHERE enum_key > 0 and lc_key like 'h%';

ALTER TABLE table_with_lc_key MODIFY COLUMN enum_key Enum('x' = 2, 'y' = 1, 'z' = 3);
ALTER TABLE table_with_lc_key MODIFY COLUMN enum_key Enum16('x' = 2, 'y' = 1, 'z' = 3); --{serverError ALTER_OF_COLUMN_IS_FORBIDDEN}
SHOW CREATE TABLE table_with_lc_key;

DETACH TABLE table_with_lc_key;
ATTACH TABLE table_with_lc_key;

SELECT * FROM table_with_lc_key WHERE enum_key > 0 and lc_key like 'h%';

ALTER TABLE table_with_lc_key MODIFY COLUMN enum_key Int8;

SHOW CREATE TABLE table_with_lc_key;

DETACH TABLE table_with_lc_key;
ATTACH TABLE table_with_lc_key;

SELECT * FROM table_with_lc_key WHERE enum_key > 0 and lc_key like 'h%';

DROP TABLE IF EXISTS table_with_lc_key;


DROP TABLE IF EXISTS table_with_string_key;
CREATE TABLE table_with_string_key
(
    int_key Int8,
    str_key String,
    value String
)
ENGINE MergeTree()
ORDER BY (int_key, str_key);

INSERT INTO table_with_string_key VALUES(1, 'hello', 'world');

ALTER TABLE table_with_string_key MODIFY COLUMN str_key LowCardinality(String);

SHOW CREATE TABLE table_with_string_key;

DETACH TABLE table_with_string_key;
ATTACH TABLE table_with_string_key;

SELECT * FROM table_with_string_key WHERE int_key > 0 and str_key like 'h%';

ALTER TABLE table_with_string_key MODIFY COLUMN int_key Enum8('y' = 1, 'x' = 2); --{serverError ALTER_OF_COLUMN_IS_FORBIDDEN}

DROP TABLE IF EXISTS table_with_string_key;
