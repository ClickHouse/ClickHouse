DROP TABLE IF EXISTS t_collisions;

SELECT lower(hex(reverse(CAST(sipHash128('very_very_long_column_name_that_will_be_replaced_with_hash'), 'FixedString(16)'))));

CREATE TABLE t_collisions
(
    `very_very_long_column_name_that_will_be_replaced_with_hash` Int32,
    `e798545eefc8b7a1c2c81ff00c064ad8` Int32
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS replace_long_file_name_to_hash = 1, max_file_name_length = 42; -- { serverError BAD_ARGUMENTS }

DROP TABLE IF EXISTS t_collisions;

CREATE TABLE t_collisions
(
    `col1` Int32,
    `e798545eefc8b7a1c2c81ff00c064ad8` Int32
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS replace_long_file_name_to_hash = 1, max_file_name_length = 42;

ALTER TABLE t_collisions ADD COLUMN very_very_long_column_name_that_will_be_replaced_with_hash Int32;  -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_collisions RENAME COLUMN col1 TO very_very_long_column_name_that_will_be_replaced_with_hash;  -- { serverError BAD_ARGUMENTS }

DROP TABLE IF EXISTS t_collisions;

CREATE TABLE t_collisions
(
    `very_very_long_column_name_that_will_be_replaced_with_hash` Int32,
    `e798545eefc8b7a1c2c81ff00c064ad8` Int32
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS replace_long_file_name_to_hash = 0;

INSERT INTO t_collisions VALUES (1, 1);

ALTER TABLE t_collisions MODIFY SETTING replace_long_file_name_to_hash = 1, max_file_name_length = 42; -- { serverError BAD_ARGUMENTS }

INSERT INTO t_collisions VALUES (2, 2);

SELECT * FROM t_collisions ORDER BY e798545eefc8b7a1c2c81ff00c064ad8;

DROP TABLE IF EXISTS t_collisions;

CREATE TABLE t_collisions
(
    `id` Int,
    `col` Array(String),
    `col.s` Array(LowCardinality(String)),
    `col.u` Array(LowCardinality(String))
)
ENGINE = MergeTree
ORDER BY id; -- { serverError BAD_ARGUMENTS }

DROP TABLE IF EXISTS t_collisions;

CREATE TABLE t_collisions
(
    `id` Int,
    `col` String,
    `col.s` Array(LowCardinality(String)),
    `col.u` Array(LowCardinality(String))
)
ENGINE = MergeTree
ORDER BY id;

ALTER TABLE t_collisions MODIFY COLUMN col Array(String); -- { serverError BAD_ARGUMENTS }

DROP TABLE IF EXISTS t_collisions;
