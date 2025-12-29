-- Test for 3-part compound identifiers (db.namespace.table)
-- This tests parsing of dotted table names like namespace.table in DataLakeCatalog style

DROP DATABASE IF EXISTS test_compound_db;
CREATE DATABASE test_compound_db;

-- Create tables with dots in names (simulating namespace.table pattern)
CREATE TABLE test_compound_db.`ns1.table1` (id UInt64, name String) ENGINE = Memory;
CREATE TABLE test_compound_db.`ns1.table2` (id UInt64, value Int32) ENGINE = Memory;
CREATE TABLE test_compound_db.`ns2.table1` (id UInt64, data String) ENGINE = Memory;

INSERT INTO test_compound_db.`ns1.table1` VALUES (1, 'Alice'), (2, 'Bob');
INSERT INTO test_compound_db.`ns1.table2` VALUES (1, 100), (2, 200);
INSERT INTO test_compound_db.`ns2.table1` VALUES (1, 'data1');

-- Test 1: SHOW TABLES shows full names with dots
SELECT '-- SHOW TABLES';
SHOW TABLES FROM test_compound_db;

-- Test 2: SELECT using 3-part identifier (db.namespace.table)
SELECT '-- SELECT with 3-part identifier';
SELECT * FROM test_compound_db.ns1.table1 ORDER BY id;

-- Test 3: EXISTS TABLE with 3-part identifier
SELECT '-- EXISTS TABLE';
EXISTS TABLE test_compound_db.ns1.table1;
EXISTS TABLE test_compound_db.ns1.nonexistent;

-- Test 4: DESCRIBE with 3-part identifier
SELECT '-- DESCRIBE';
DESCRIBE test_compound_db.ns1.table1;

-- Test 5: 2-part identifier still works (quoted name with dot)
SELECT '-- 2-part with quoted name';
SELECT * FROM test_compound_db.`ns1.table1` ORDER BY id;

-- Test 6: Cross-namespace join using 3-part identifiers
SELECT '-- Cross-namespace join';
SELECT t1.name, t2.value 
FROM test_compound_db.ns1.table1 AS t1 
JOIN test_compound_db.ns1.table2 AS t2 ON t1.id = t2.id 
ORDER BY t1.id;

-- Test 7: SELECT from different namespace  
SELECT '-- Different namespace';
SELECT * FROM test_compound_db.ns2.table1;

-- Cleanup
DROP DATABASE test_compound_db;
