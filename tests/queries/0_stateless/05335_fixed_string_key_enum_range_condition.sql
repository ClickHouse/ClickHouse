-- A `FixedString` primary key compared with an `Enum` is compared zero-padded, and the primary key
-- is used for the order comparisons as well as for equality.

DROP TABLE IF EXISTS t_fs_enum;

CREATE TABLE t_fs_enum (k FixedString(3)) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1;
INSERT INTO t_fs_enum SELECT toString(number) FROM numbers(100);

SET force_primary_key = 1;

SELECT count() FROM t_fs_enum WHERE k = CAST('95' AS Enum8('95' = 1, '7' = 2)) SETTINGS max_rows_to_read = 2;
SELECT count() FROM t_fs_enum WHERE k != CAST('95' AS Enum8('95' = 1, '7' = 2));
SELECT count() FROM t_fs_enum WHERE k > CAST('95' AS Enum8('95' = 1, '7' = 2)) SETTINGS max_rows_to_read = 6;
SELECT count() FROM t_fs_enum WHERE k >= CAST('95' AS Enum8('95' = 1, '7' = 2)) SETTINGS max_rows_to_read = 6;
SELECT count() FROM t_fs_enum WHERE k < CAST('11' AS Enum8('11' = 1, '7' = 2)) SETTINGS max_rows_to_read = 6;
SELECT count() FROM t_fs_enum WHERE k <= CAST('11' AS Enum8('11' = 1, '7' = 2)) SETTINGS max_rows_to_read = 6;

-- The same results without the primary key.
SET force_primary_key = 0;
SELECT count() FROM t_fs_enum WHERE k > CAST('95' AS Enum8('95' = 1, '7' = 2)) SETTINGS use_primary_key = 0;
SELECT count() FROM t_fs_enum WHERE k <= CAST('11' AS Enum8('11' = 1, '7' = 2)) SETTINGS use_primary_key = 0;

-- A value longer than the key does not use the primary key.
SELECT count() FROM t_fs_enum WHERE k < CAST('9999' AS Enum8('9999' = 1, '7' = 2));

DROP TABLE t_fs_enum;
