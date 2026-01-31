-- Tags: no-fasttest, no-parallel
-- no-fasttest: using SYSTEM STOP MERGES
-- no-replicated-database: using SYSTEM STOP MERGES

SYSTEM STOP MERGES;


-- Source table, unpartitioned

DROP TABLE IF EXISTS table_src;
CREATE TABLE table_src
(
    order       Int64,
    random_date Date
)
    ENGINE = MergeTree()
        ORDER BY order;


-- Destination table, partitioned by (year,month)

DROP TABLE IF EXISTS table_dst;
CREATE TABLE table_dst
(
    order       Int64,
    random_date Date
)
    ENGINE = MergeTree()
        PARTITION BY toStartOfMonth(random_date)
        ORDER BY tuple();

ALTER TABLE table_dst MODIFY SETTING old_parts_lifetime=1;


-- Prepare data with random dates

INSERT INTO table_src
SELECT number AS order, toDate(rand() % 3650) AS random_date
FROM numbers(2000000);


-- Check the default error when inserting many parts

INSERT INTO table_dst -- { serverError TOO_MANY_PARTS }
SELECT *
FROM table_src;


-- Check that too many parts are created indeed

INSERT INTO table_dst
    SETTINGS max_insert_threads=1, throw_on_max_partitions_per_insert_block=0
SELECT *
FROM table_src;

SELECT COUNT(*) -- >=240
FROM system.parts
WHERE database = currentDatabase() AND table = 'table_dst';


-- Collect garbage

TRUNCATE TABLE table_dst;
OPTIMIZE TABLE table_dst FINAL;
SELECT sleep(3) FORMAT Null; -- Wait for old_parts_lifetime to pass

SELECT COUNT(*) -- <5
FROM system.parts
WHERE database = currentDatabase() AND table = 'table_dst';


-- Check that unnecessary parts are not created with the setting

INSERT INTO table_dst
    SETTINGS max_insert_threads=1, throw_on_max_partitions_per_insert_block=0, max_insert_parts_buffer_rows=0
SELECT *
FROM table_src;

SELECT COUNT(*) FROM table_dst; -- =2000000
SELECT COUNT(*) FROM system.parts -- <200
WHERE database = currentDatabase() AND table = 'table_dst';


-- Collect garbage

TRUNCATE TABLE table_dst;
OPTIMIZE TABLE table_dst FINAL;
SELECT sleep(3) FORMAT Null; -- Wait for old_parts_lifetime to pass

SELECT COUNT(*) -- <5
FROM system.parts
WHERE database = currentDatabase() AND table = 'table_dst';


-- Check that some additional parts are created when the buffer is small

INSERT INTO table_dst
    SETTINGS max_insert_threads=1, throw_on_max_partitions_per_insert_block=0, max_insert_parts_buffer_rows=1
SELECT *
FROM table_src;

SELECT COUNT(*) FROM table_dst; -- =2000000
SELECT COUNT(*) FROM system.parts -- >200
WHERE database = currentDatabase() AND table = 'table_dst';

OPTIMIZE TABLE table_dst FINAL;
SELECT sleep(3) FORMAT Null;

SELECT COUNT(*) FROM system.parts -- =120
WHERE database = currentDatabase() AND table = 'table_dst';
