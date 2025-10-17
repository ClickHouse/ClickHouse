SET output_format_write_statistics = 0;

DROP TABLE IF EXISTS 03408_unsorted;

CREATE TABLE 03408_unsorted (id Int32, val String) ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part=1
AS
SELECT number % 10, leftPad(toString(number), 2, '0') FROM numbers(50);

SELECT '-- Assert total number of groups and records in unsorted';
SELECT uniqExact(id), count() FROM 03408_unsorted;

SELECT '';
SELECT '-- Assert rows_before_limit for unsorted ORDER BY + LIMIT BY + LIMIT';

SELECT id, val FROM 03408_unsorted ORDER BY id, val LIMIT 1 BY id LIMIT 3
FORMAT JsonCompact SETTINGS max_block_size=1, exact_rows_before_limit=0;

SELECT '';
SELECT '-- Assert rows_before_limit for unsorted ORDER BY + LIMIT BY + LIMIT, exact';

SELECT id, val FROM 03408_unsorted ORDER BY id, val LIMIT 1 BY id LIMIT 3
FORMAT JsonCompact SETTINGS max_block_size=1, exact_rows_before_limit=1;

SELECT '';
SELECT '-- Assert rows_before_limit for unsorted HAVING + ORDER BY + LIMIT BY + LIMIT, exact';

SELECT id, val FROM 03408_unsorted GROUP BY id, val HAVING id < 7 ORDER BY id, val DESC LIMIT 1 BY id LIMIT 3
FORMAT JsonCompact SETTINGS max_block_size=1, exact_rows_before_limit=1;

DROP TABLE 03408_unsorted;

DROP TABLE IF EXISTS 03408_sorted;

CREATE TABLE 03408_sorted (id Int32, val String) ENGINE = MergeTree ORDER BY (id, val) SETTINGS min_bytes_for_wide_part=1
AS
SELECT number % 10, leftPad(toString(number), 2, '0') FROM numbers(50);

SELECT '';
SELECT '-- Assert total number of groups and records in sorted';
SELECT uniqExact(id), count() FROM 03408_sorted;

SELECT '';
SELECT '-- Assert rows_before_limit for sorted ORDER BY + LIMIT BY + LIMIT';

SELECT id, val FROM 03408_sorted ORDER BY id, val LIMIT 1 BY id LIMIT 3
FORMAT JsonCompact SETTINGS max_block_size=1, exact_rows_before_limit=0;

SELECT '';
SELECT '-- Assert rows_before_limit for sorted ORDER BY + LIMIT BY + LIMIT, exact';

SELECT id, val FROM 03408_sorted ORDER BY id, val LIMIT 1 BY id LIMIT 3
FORMAT JsonCompact SETTINGS max_block_size=1, exact_rows_before_limit=1;

SELECT '';
SELECT '-- Assert rows_before_limit for sorted HAVING + ORDER BY + LIMIT BY + LIMIT, exact';

SELECT id, val FROM 03408_sorted GROUP BY id, val HAVING id < 7 ORDER BY id, val DESC LIMIT 1 BY id LIMIT 3
FORMAT JsonCompact SETTINGS max_block_size=1, exact_rows_before_limit=1;

DROP TABLE 03408_sorted;
