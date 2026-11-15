-- { echo }

DROP TABLE IF EXISTS source_table;
DROP TABLE IF EXISTS alias_syntax_1;
DROP TABLE IF EXISTS alias_syntax_2;

SET allow_experimental_alias_table_engine = 1;

-- Create source table
CREATE TABLE source_table (id UInt32, name String, value Float64)
ENGINE = MergeTree ORDER BY id;

INSERT INTO source_table VALUES (1, 'one', 10.1), (2, 'two', 20.2), (3, 'three', 30.3);

-- Syntax: ENGINE = Alias(table)
SELECT 'Test ENGINE = Alias(table)';
CREATE TABLE alias_syntax_1 ENGINE = Alias('source_table');
SELECT * FROM alias_syntax_1 ORDER BY id;

-- Syntax: ENGINE = Alias(db, table)
SELECT 'Test ENGINE = Alias(db, table)';
CREATE TABLE alias_syntax_2 ENGINE = Alias(currentDatabase(), 'source_table');
SELECT * FROM alias_syntax_2 ORDER BY id;

-- Test: All aliases work identically
SELECT 'Test All aliases work identically';
INSERT INTO alias_syntax_1 VALUES (4, 'four', 40.4);
SELECT count() FROM source_table;
SELECT count() FROM alias_syntax_1;
SELECT count() FROM alias_syntax_2;

INSERT INTO alias_syntax_2 VALUES (5, 'five', 50.5);
SELECT count() FROM source_table;

-- Test: with explicit columns (should fail)
SELECT 'Test Explicit columns should fail';
CREATE TABLE alias_syntax_3 (id UInt32, name String, value Float64) ENGINE = Alias('source_table'); -- { serverError BAD_ARGUMENTS }

-- Test: Alias to alias
DROP TABLE IF EXISTS base_table;
DROP TABLE IF EXISTS alias_1;
DROP TABLE IF EXISTS alias_2;

CREATE TABLE base_table (id UInt32, value String) ENGINE = MergeTree ORDER BY id;
CREATE TABLE alias_1 ENGINE = Alias('base_table');
CREATE TABLE alias_2 ENGINE = Alias('alias_1'); -- { serverError BAD_ARGUMENTS }

