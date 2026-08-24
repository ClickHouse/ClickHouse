-- Tags: no-parallel-replicas

SET output_format_write_statistics = 0;

DROP TABLE IF EXISTS 03408_memory;

CREATE TABLE 03408_memory (id Int32, val String) ENGINE = Memory
AS
SELECT number % 10, leftPad(toString(number), 2, '0') FROM numbers(50);

SELECT '-- Assert total number of groups and records in memory';
SELECT uniqExact(id), count() FROM 03408_memory;

SELECT '';
SELECT '-- Assert rows_before_limit for memory ORDER BY + LIMIT BY + LIMIT, exact';
SELECT id, val FROM 03408_memory ORDER BY id, val LIMIT 1 BY id LIMIT 3
FORMAT JsonCompact SETTINGS exact_rows_before_limit=1;

SELECT '';
SELECT '-- Assert rows_before_limit for memory HAVING + ORDER BY + LIMIT BY + LIMIT, exact';
SELECT id, val FROM 03408_memory GROUP BY id, val HAVING id < 7 ORDER BY id, val DESC LIMIT 1 BY id LIMIT 3
FORMAT JsonCompact SETTINGS exact_rows_before_limit=1;

DROP TABLE 03408_memory;
