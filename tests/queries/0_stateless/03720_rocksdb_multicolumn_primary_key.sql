-- Tags: no-ordinary-database, no-fasttest
-- Tag no-ordinary-database: Sometimes cannot lock file most likely due to concurrent or adjacent tests, but we don't care how it works in Ordinary database
-- Tag no-fasttest: In fasttest, ENABLE_LIBRARIES=0, so rocksdb engine is not enabled by default

DROP TABLE IF EXISTS 03720_test;
CREATE TABLE 03720_test (k1 UInt64, k2 UInt64, val UInt64) ENGINE=EmbeddedRocksDB PRIMARY KEY (k1, k2);
INSERT INTO 03720_test SELECT number %10, number, number FROM numbers(100);

SELECT COUNT(1) == 100 FROM 03720_test;    -- Full scan
SELECT COUNT(1) == 5 FROM 03720_test WHERE k1 = 5 AND k2 IN (15, 35, 55, 75, 95);   -- Key scan
SELECT COUNT(1) == 0 FROM 03720_test WHERE k1 = 6 AND k2 IN (15, 35, 55, 75, 95);
SELECT COUNT(1) == 1 FROM 03720_test WHERE k1 IN (1, 3, 5) AND k2 = 15 AND k1 IN (3, 5, 7);
SELECT COUNT(1) == 0 FROM 03720_test WHERE k1 IN (1, 3, 5) AND k2 = 15 AND k1 IN (2, 4, 6);
SELECT COUNT(1) == 6 FROM 03720_test WHERE (k1 IN (1, 3, 5) OR k1 IN (2, 3, 4, 5, 6)) AND k2 IN (11, 13, 15, 12, 14, 16);

DROP TABLE 03720_test;

-- Tuple equality
DROP TABLE IF EXISTS 03720_tuple_equality;
CREATE TABLE 03720_tuple_equality (k1 UInt32, k2 UInt32, val String) ENGINE=EmbeddedRocksDB PRIMARY KEY (k1, k2);
INSERT INTO 03720_tuple_equality VALUES (1, 10, 'a'), (1, 20, 'b'), (2, 10, 'c'), (2, 20, 'd');

SELECT val FROM 03720_tuple_equality WHERE (k1, k2) = (1, 10) ORDER BY val;
SELECT val FROM 03720_tuple_equality WHERE (k1, k2) = (2, 20) ORDER BY val;
SELECT COUNT(*) FROM 03720_tuple_equality WHERE (k1, k2) = (3, 30); -- non-existent
SELECT COUNT(*) FROM 03720_tuple_equality WHERE k1 = 1 AND k2 IN ();

DROP TABLE 03720_tuple_equality;

-- Tuple IN syntax
DROP TABLE IF EXISTS 03720_tuple_in;
CREATE TABLE 03720_tuple_in (k1 UInt32, k2 UInt32, val String) ENGINE=EmbeddedRocksDB PRIMARY KEY (k1, k2);
INSERT INTO 03720_tuple_in VALUES (1, 10, 'a'), (1, 20, 'b'), (2, 10, 'c'), (2, 20, 'd'), (3, 30, 'e');

SELECT val FROM 03720_tuple_in WHERE (k1, k2) IN ((1, 10), (2, 20), (3, 30)) ORDER BY val;
SELECT COUNT(*) FROM 03720_tuple_in WHERE (k1, k2) IN ((1, 10), (5, 50)); -- partial match

DROP TABLE 03720_tuple_in;

-- Three-column primary key
DROP TABLE IF EXISTS 03720_three_columns;
CREATE TABLE 03720_three_columns (k1 UInt32, k2 UInt32, k3 UInt32, val String) ENGINE=EmbeddedRocksDB PRIMARY KEY (k1, k2, k3);
INSERT INTO 03720_three_columns VALUES (1, 1, 1, 'a'), (1, 1, 2, 'b'), (1, 2, 1, 'c'), (2, 1, 1, 'd');

SELECT val FROM 03720_three_columns WHERE (k1, k2, k3) = (1, 1, 1) ORDER BY val;
SELECT val FROM 03720_three_columns WHERE (k1, k2, k3) IN ((1, 1, 1), (2, 1, 1)) ORDER BY val;
SELECT COUNT(*) FROM 03720_three_columns WHERE k1 = 1 AND k2 = 1 AND k3 IN (1, 2); -- Cartesian with 3 keys

DROP TABLE 03720_three_columns;

-- Four-column
DROP TABLE IF EXISTS 03720_four_columns;
CREATE TABLE 03720_four_columns (k1 UInt32, k2 UInt32, k3 UInt32, k4 UInt32, val String) ENGINE=EmbeddedRocksDB PRIMARY KEY (k1, k2, k3, k4);
INSERT INTO 03720_four_columns VALUES (1,2,3,4,'a'), (1,2,3,5,'b');

SELECT val FROM 03720_four_columns WHERE (k1, k2, k3, k4) = (1, 2, 3, 4);
SELECT COUNT(*) FROM 03720_four_columns WHERE k1 = 1 AND k2 = 2 AND k3 = 3 AND k4 IN (4, 5);

DROP TABLE 03720_four_columns;

-- String type
DROP TABLE IF EXISTS 03720_string_keys;
CREATE TABLE 03720_string_keys (k1 String, k2 String, val UInt32) ENGINE=EmbeddedRocksDB PRIMARY KEY (k1, k2);
INSERT INTO 03720_string_keys VALUES ('foo', 'bar', 1), ('foo', 'baz', 2), ('qux', 'bar', 3);

SELECT val FROM 03720_string_keys WHERE (k1, k2) = ('foo', 'bar') ORDER BY val;
SELECT val FROM 03720_string_keys WHERE k1 = 'foo' AND k2 IN ('bar', 'baz') ORDER BY val;
SELECT COUNT(*) FROM 03720_string_keys WHERE (k1, k2) IN (('foo', 'bar'), ('qux', 'bar'));

DROP TABLE 03720_string_keys;

-- Mixed types
DROP TABLE IF EXISTS 03720_mixed_types;
CREATE TABLE 03720_mixed_types (k1 UInt64, k2 String, val Float32) ENGINE=EmbeddedRocksDB PRIMARY KEY (k1, k2);
INSERT INTO 03720_mixed_types VALUES (100, 'a', 1.5), (100, 'b', 2.5), (200, 'a', 3.5);

SELECT val FROM 03720_mixed_types WHERE (k1, k2) = (100, 'a') ORDER BY val;
SELECT val FROM 03720_mixed_types WHERE k1 = 100 AND k2 IN ('a', 'b') ORDER BY val;

DROP TABLE 03720_mixed_types;

-- DateTime
DROP TABLE IF EXISTS 03720_datetime;
CREATE TABLE 03720_datetime (k1 UInt32, k2 DateTime, val String) ENGINE=EmbeddedRocksDB PRIMARY KEY (k1, k2);
INSERT INTO 03720_datetime VALUES (1, '2024-01-01 00:00:00', 'a'), (1, '2024-01-02 00:00:00', 'b');

SELECT val FROM 03720_datetime WHERE (k1, k2) = (1, '2024-01-01 00:00:00');

DROP TABLE 03720_datetime;

-- Enum types
DROP TABLE IF EXISTS 03720_enum;
CREATE TABLE 03720_enum (k1 Enum8('a'=1, 'b'=2), k2 UInt32, val String) ENGINE=EmbeddedRocksDB PRIMARY KEY (k1, k2);
INSERT INTO 03720_enum VALUES ('a', 1, 'test1'), ('b', 2, 'test2');

SELECT val FROM 03720_enum WHERE (k1, k2) = ('a', 1);

DROP TABLE 03720_enum;

-- Single column key (backward compatibility)
DROP TABLE IF EXISTS 03720_single_column;
CREATE TABLE 03720_single_column (k1 UInt64, val String) ENGINE=EmbeddedRocksDB PRIMARY KEY k1;
INSERT INTO 03720_single_column VALUES (1, 'a'), (2, 'b'), (3, 'c');

SELECT val FROM 03720_single_column WHERE k1 = 1 ORDER BY val;
SELECT val FROM 03720_single_column WHERE k1 IN (1, 3) ORDER BY val;

DROP TABLE 03720_single_column;

-- Cartesian product: 5x3 = 15 lookups
DROP TABLE IF EXISTS 03720_cartesian;
CREATE TABLE 03720_cartesian (k1 UInt32, k2 UInt32, val String) ENGINE=EmbeddedRocksDB PRIMARY KEY (k1, k2);
INSERT INTO 03720_cartesian VALUES
    (0,0,'0'), (0,1,'1'), (0,2,'2'),
    (1,0,'3'), (1,1,'4'), (1,2,'5'),
    (2,0,'6'), (2,1,'7'), (2,2,'8'),
    (3,0,'9'), (3,1,'10'), (3,2,'11'),
    (4,0,'12'), (4,1,'13'), (4,2,'14'),
    (5,0,'15'), (5,1,'16'), (5,2,'17'), (5,3,'18'), (5,4,'19');

SELECT COUNT(*) FROM 03720_cartesian WHERE k1 IN (0, 1, 2, 3, 4) AND k2 IN (0, 1, 2);
SELECT COUNT(*) FROM 03720_cartesian WHERE k1 = 5 AND k2 IN (0, 1, 2, 3, 4);

DROP TABLE 03720_cartesian;

-- Large Cartesian product
DROP TABLE IF EXISTS 03720_large_cartesian;
CREATE TABLE 03720_large_cartesian (k1 UInt32, k2 UInt32, val String) ENGINE=EmbeddedRocksDB PRIMARY KEY (k1, k2);
INSERT INTO 03720_large_cartesian SELECT intDiv(number, 100), number % 100, toString(number) FROM numbers(10000);

SELECT COUNT(*) FROM 03720_large_cartesian WHERE k1 IN (0,1,2,3,4,5,6,7,8,9) AND k2 IN (0,1,2,3,4,5,6,7,8,9);

DROP TABLE 03720_large_cartesian;

-- Empty result sets
DROP TABLE IF EXISTS 03720_empty_results;
CREATE TABLE 03720_empty_results (k1 UInt32, k2 UInt32, val String) ENGINE=EmbeddedRocksDB PRIMARY KEY (k1, k2);
INSERT INTO 03720_empty_results VALUES (1, 1, 'a');

SELECT COUNT(*) FROM 03720_empty_results WHERE (k1, k2) = (2, 2);
SELECT COUNT(*) FROM 03720_empty_results WHERE (k1, k2) IN ((2, 2), (3, 3));
SELECT COUNT(*) FROM 03720_empty_results WHERE k1 = 1 AND k2 = 2;

DROP TABLE 03720_empty_results;

-- OR conditions
DROP TABLE IF EXISTS 03720_complex_or;
CREATE TABLE 03720_complex_or (k1 UInt32, k2 UInt32, val String) ENGINE=EmbeddedRocksDB PRIMARY KEY (k1, k2);
INSERT INTO 03720_complex_or VALUES (1, 1, 'a'), (1, 2, 'b'), (2, 1, 'c'), (2, 2, 'd');

SELECT COUNT(*) FROM 03720_complex_or WHERE (k1 = 1 AND k2 = 1) OR (k1 = 2 AND k2 = 2);
SELECT COUNT(*) FROM 03720_complex_or WHERE (k1, k2) IN ((1, 1), (2, 2));

DROP TABLE 03720_complex_or;

-- Update and upsert operations with multi-column keys
DROP TABLE IF EXISTS 03720_mutations;
CREATE TABLE 03720_mutations (k1 UInt32, k2 UInt32, val String) ENGINE=EmbeddedRocksDB PRIMARY KEY (k1, k2);
INSERT INTO 03720_mutations VALUES (1, 1, 'old'), (1, 2, 'data');

-- Update by overwriting (RocksDB's upsert behavior)
INSERT INTO 03720_mutations VALUES (1, 1, 'new');
SELECT val FROM 03720_mutations WHERE (k1, k2) = (1, 1) ORDER BY val;

DROP TABLE 03720_mutations;

-- Partial key filter (should trigger full scan)
DROP TABLE IF EXISTS 03720_partial_key;
CREATE TABLE 03720_partial_key (k1 UInt32, k2 UInt32, k3 UInt32, val String) ENGINE=EmbeddedRocksDB PRIMARY KEY (k1, k2, k3);
INSERT INTO 03720_partial_key SELECT intDiv(number, 10), number % 10, number % 3, toString(number) FROM numbers(100);

-- Only k1 specified - should trigger full scan
SELECT COUNT(*) FROM 03720_partial_key WHERE k1 = 1;

DROP TABLE 03720_partial_key;

