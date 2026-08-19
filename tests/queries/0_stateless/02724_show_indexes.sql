-- Tags: no-parallel
-- no-parallel: creates a custom database schema and expects to use it exclusively

-- Create a test table and verify that the output of SHOW INDEXES is sane.
-- The matching of actual/expected results relies on the fact that the output of SHOW INDEX is sorted.
DROP TABLE IF EXISTS tbl;
CREATE TABLE tbl
(
    a UInt64,
    b UInt64,
    c UInt64,
    d UInt64,
    e UInt64,
    INDEX mm1_idx (a, c, d) TYPE minmax,
    INDEX mm2_idx (c, d, e) TYPE minmax,
    INDEX set_idx (e)       TYPE set(100),
    INDEX blf_idx (d, b)    TYPE bloom_filter(0.8)
)
ENGINE = MergeTree
PRIMARY KEY (c, a);

SELECT '--- Aliases of SHOW INDEX';
SHOW INDEX FROM tbl;
SHOW INDEXES FROM tbl;
SHOW INDICES FROM tbl;
SHOW KEYS FROM tbl;

SELECT '--- EXTENDED';
SHOW EXTENDED INDEX FROM tbl;
--
SELECT '--- WHERE';
SHOW INDEX FROM tbl WHERE index_type LIKE '%minmax%';

SELECT '--- Check with weird table names';

DROP TABLE IF EXISTS `$4@^7`;
CREATE TABLE `$4@^7` (c String) ENGINE = MergeTree ORDER BY c;
SHOW INDEX FROM `$4@^7`;
DROP TABLE `$4@^7`;

DROP TABLE IF EXISTS NULL;
CREATE TABLE NULL (c String) ENGINE = MergeTree ORDER BY c;
SHOW INDEX FROM NULL;
DROP TABLE NULL;

DROP TABLE IF EXISTS `tab.with.dots`;
CREATE TABLE `tab.with.dots`
(
    a UInt64,
    b UInt64,
    c UInt64,
    d UInt64,
    e UInt64,
    INDEX mm1_idx (a, c, d) TYPE minmax,
    INDEX mm2_idx (c, d, e) TYPE minmax,
    INDEX set_idx (e)       TYPE set(100),
    INDEX blf_idx (d, b)    TYPE bloom_filter(0.8)
)
ENGINE = MergeTree
PRIMARY KEY (c, a);
SHOW INDEX FROM `tab.with.dots`;
DROP TABLE `tab.with.dots`;

DROP DATABASE IF EXISTS `'`;
CREATE DATABASE `'`;
CREATE TABLE `'`.`'` (c String) ENGINE = MergeTree ORDER BY c;
SHOW INDEX FROM `'` FROM `'`;
SHOW INDEX FROM `'`.`'`; -- abbreviated form
DROP TABLE `'`.`'`;
DROP DATABASE `'`;

-- Create a table in a different database. Intentionally using the same table/column names as above so
-- we notice if something is buggy in the implementation of SHOW INDEX.
DROP DATABASE IF EXISTS database_123456789abcde;
CREATE DATABASE database_123456789abcde; -- pseudo-random database name

DROP TABLE IF EXISTS database_123456789abcde.tbl;
CREATE TABLE database_123456789abcde.tbl
(
    a UInt64,
    b UInt64,
    INDEX mmi_idx b TYPE minmax
)
ENGINE = MergeTree
PRIMARY KEY a;

SELECT '--- Original table';
SHOW INDEX FROM tbl;

SELECT '--- Equally named table in other database';
SHOW INDEX FROM tbl FROM database_123456789abcde;

SELECT '--- Short form';
SHOW INDEX FROM database_123456789abcde.tbl;

DROP DATABASE database_123456789abcde;

DROP TABLE tbl;
