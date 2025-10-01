DROP TABLE IF EXISTS source_table;
DROP TABLE IF EXISTS alias_syntax_1;
DROP TABLE IF EXISTS alias_syntax_2;
DROP TABLE IF EXISTS alias_syntax_3;

-- Create source table
CREATE TABLE source_table (id UInt32, name String, value Float64) 
ENGINE = MergeTree ORDER BY id;

-- Insert test data
INSERT INTO source_table VALUES (1, 'one', 10.1), (2, 'two', 20.2), (3, 'three', 30.3);

-- Syntax: ENGINE = Alias(table)
SELECT 'Test ENGINE = Alias(table)';
CREATE TABLE alias_syntax_1 ENGINE = Alias('source_table');
SELECT * FROM alias_syntax_1 ORDER BY id;

-- Syntax: ENGINE = Alias(db, table)
SELECT 'Test ENGINE = Alias(db, table)';
CREATE TABLE alias_syntax_2 ENGINE = Alias(currentDatabase(), 'source_table');
SELECT * FROM alias_syntax_2 ORDER BY id;

-- Syntax: Explicit columns
SELECT 'Test Explicit columns';
CREATE TABLE alias_syntax_3 (id UInt32, name String, value Float64) 
ENGINE = Alias('source_table');
SELECT * FROM alias_syntax_3 ORDER BY id;

-- Test: All aliases work identically
SELECT 'Test All aliases work identically';
INSERT INTO alias_syntax_1 VALUES (4, 'four', 40.4);
SELECT count() FROM source_table;
SELECT count() FROM alias_syntax_1;
SELECT count() FROM alias_syntax_2;
SELECT count() FROM alias_syntax_3;

-- Test: Operations through different aliases
SELECT 'Test Operations through aliases';
INSERT INTO alias_syntax_2 VALUES (5, 'five', 50.5);
SELECT count() FROM source_table;

DROP TABLE IF EXISTS alias_syntax_1;
DROP TABLE IF EXISTS alias_syntax_2;
DROP TABLE IF EXISTS alias_syntax_3;
DROP TABLE IF EXISTS source_table;

DROP DATABASE IF EXISTS test_db1 SYNC;
DROP DATABASE IF EXISTS test_db2 SYNC;
CREATE DATABASE test_db1;
CREATE DATABASE test_db2;

-- Create source tables
CREATE TABLE test_db1.source1 (id UInt32, value String) ENGINE = MergeTree ORDER BY id;
CREATE TABLE test_db2.source2 (id UInt32, name String, score Float64) ENGINE = MergeTree ORDER BY id;

-- Insert test data
INSERT INTO test_db1.source1 VALUES (1, 'one'), (2, 'two');
INSERT INTO test_db2.source2 VALUES (10, 'ten', 10.5), (20, 'twenty', 20.5);

-- Test: ENGINE = Alias(t) same database
SELECT 'Test ENGINE = Alias(t)';
CREATE TABLE test_db1.alias_3_1 ENGINE = Alias('source1');
SELECT * FROM test_db1.alias_3_1 ORDER BY id;

-- Test: ENGINE = Alias(db.t) format
SELECT 'Test ENGINE = Alias(db.t)';
CREATE TABLE test_db1.alias_3_2 ENGINE = Alias('test_db2.source2');
SELECT * FROM test_db1.alias_3_2 ORDER BY id;

-- Test: ENGINE = Alias(db, t) two-parameter
SELECT 'Test ENGINE = Alias(db, t)';
CREATE TABLE test_db1.alias_4_2 ENGINE = Alias('test_db2', 'source2');
SELECT * FROM test_db1.alias_4_2 ORDER BY id;

-- Test: Write through cross-database aliases
SELECT 'Test Write operations';
INSERT INTO test_db1.alias_3_1 VALUES (3, 'three');
INSERT INTO test_db1.alias_3_2 VALUES (30, 'thirty', 30.5);
SELECT count() FROM test_db1.source1;
SELECT count() FROM test_db2.source2;

-- Test: Verify all aliases
SELECT 'Test Count verification';
SELECT count() FROM test_db1.alias_3_1;
SELECT count() FROM test_db1.alias_3_2;
SELECT count() FROM test_db1.alias_4_2;

DROP DATABASE IF EXISTS test_db1 SYNC;
DROP DATABASE IF EXISTS test_db2 SYNC;
