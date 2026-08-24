-- Tags: no-parallel
-- no-parallel: creates a custom database schema and expects to use it exclusively

-- Create a test table and verify that the output of SHOW COLUMNS is sane.
-- The matching of actual/expected results relies on the fact that the output of SHOW COLUMNS is sorted.
DROP TABLE IF EXISTS tab;
CREATE TABLE tab
(
    `uint64` UInt64,
    `int32` Nullable(Int32) COMMENT 'example comment',
    `str` String,
    INDEX idx str TYPE set(1000)
)
ENGINE = MergeTree
PRIMARY KEY (uint64)
ORDER BY (uint64, str);

SELECT '--- Aliases of SHOW COLUMNS';
SHOW COLUMNS FROM tab;
SHOW FIELDS FROM tab;

SELECT '--- EXTENDED';
SHOW EXTENDED COLUMNS FROM tab;

SELECT '--- FULL';
SHOW FULL COLUMNS FROM tab;

SELECT '--- LIKE';
SHOW COLUMNS FROM tab LIKE '%int%';

SELECT '--- NOT LIKE';
SHOW COLUMNS FROM tab NOT LIKE '%int%';

SELECT '--- ILIKE';
SHOW COLUMNS FROM tab ILIKE '%INT%';

SELECT '--- NOT ILIKE';
SHOW COLUMNS FROM tab NOT ILIKE '%INT%';

SELECT '--- WHERE';
SHOW COLUMNS FROM tab WHERE field LIKE '%int%';

SELECT '--- LIMIT';
SHOW COLUMNS FROM tab LIMIT 1;

SELECT '--- Check with weird table names';

DROP TABLE IF EXISTS `$4@^7`;
CREATE TABLE `$4@^7` (c String) ENGINE = MergeTree ORDER BY c;
SHOW COLUMNS FROM `$4@^7`;
DROP TABLE `$4@^7`;

DROP TABLE IF EXISTS NULL;
CREATE TABLE NULL (c String) ENGINE = MergeTree ORDER BY c;
SHOW COLUMNS FROM NULL;
DROP TABLE NULL;

DROP TABLE IF EXISTS `tab.with.dots`;
CREATE TABLE `tab.with.dots` (c String) ENGINE = MergeTree ORDER BY c;
SHOW COLUMNS FROM `tab.with.dots`;
DROP TABLE `tab.with.dots`;

DROP DATABASE IF EXISTS `'`;
CREATE DATABASE `'`;
CREATE TABLE `'`.`'` (c String) ENGINE = MergeTree ORDER BY c;
SHOW COLUMNS FROM `'` FROM `'`;
SHOW COLUMNS FROM `'`.`'`; -- abbreviated form
DROP TABLE `'`.`'`;
DROP DATABASE `'`;

-- Create a table in a different database. Intentionally useing the same table/column names as above so
-- we notice if something is buggy in the implementation of SHOW COLUMNS.
DROP DATABASE IF EXISTS database_123456789abcde;
CREATE DATABASE database_123456789abcde; -- pseudo-random database name

DROP TABLE IF EXISTS database_123456789abcde.tab;
CREATE TABLE database_123456789abcde.tab
(
    `uint64` UInt64,
    `int32` Int32,
    `str` String
)
ENGINE = MergeTree
ORDER BY uint64;

SELECT '--- Original table';
SHOW COLUMNS FROM tab;

SELECT '--- Equally named table in other database';
SHOW COLUMNS FROM tab FROM database_123456789abcde;

SELECT '--- Short form';
SHOW COLUMNS FROM database_123456789abcde.tab;

DROP DATABASE database_123456789abcde;

DROP TABLE tab;
