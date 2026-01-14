-- Tags: no-parallel

DROP DATABASE IF EXISTS TestDBCase;
CREATE DATABASE TestDBCase;
CREATE TABLE TestDBCase.TestTableCase (Id UInt8, Name String) ENGINE = Memory;
INSERT INTO TestDBCase.TestTableCase VALUES (1, 'Alice'), (2, 'Bob');

SET case_insensitive_names = 'default';
SELECT '--- Default mode: case-sensitive ---';

-- Exact case works
SELECT * FROM TestDBCase.TestTableCase ORDER BY Id;

-- Wrong case fails (expect error, so we use a subquery trick to show it gracefully)
SELECT '--- Default mode: wrong case should fail ---';
SELECT * FROM testdbcase.testtablecase ORDER BY Id; -- { serverError UNKNOWN_DATABASE }

SET case_insensitive_names = 'standard';
SELECT '--- Standard mode: unquoted case-insensitive ---';

-- All these should work (unquoted = case-insensitive)
SELECT * FROM testdbcase.testtablecase ORDER BY Id;
SELECT * FROM TESTDBCASE.TESTTABLECASE ORDER BY Id;
SELECT * FROM TestDBCase.TestTableCase ORDER BY Id;
SELECT * FROM testDBcase.testTABLEcase ORDER BY Id;

SELECT '--- Standard mode: double-quoted database case-sensitive ---';

-- Correct case with double quotes works
SELECT * FROM "TestDBCase".testtablecase ORDER BY Id;

-- Wrong case with double quotes fails
SELECT * FROM "testdbcase".testtablecase ORDER BY Id; -- { serverError UNKNOWN_DATABASE }
SELECT * FROM "TESTDBCASE".testtablecase ORDER BY Id; -- { serverError UNKNOWN_DATABASE }

SELECT '--- Standard mode: double-quoted table case-sensitive ---';

-- Correct case with double quotes works
SELECT * FROM testdbcase."TestTableCase" ORDER BY Id;

-- Wrong case with double quotes fails
SELECT * FROM testdbcase."testtablecase" ORDER BY Id; -- { serverError UNKNOWN_TABLE }
SELECT * FROM testdbcase."TESTTABLECASE" ORDER BY Id; -- { serverError UNKNOWN_TABLE }

SELECT '--- Standard mode: mixed quoting ---';

-- DB case-insensitive (unquoted), table case-sensitive (double-quoted) - correct case works
SELECT * FROM testdbcase."TestTableCase" ORDER BY Id;
SELECT * FROM TESTDBCASE."TestTableCase" ORDER BY Id;

-- DB case-sensitive (double-quoted), table case-insensitive (unquoted) - correct db case works
SELECT * FROM "TestDBCase".testtablecase ORDER BY Id;
SELECT * FROM "TestDBCase".TESTTABLECASE ORDER BY Id;

-- Both double-quoted with correct case works
SELECT * FROM "TestDBCase"."TestTableCase" ORDER BY Id;

SELECT '--- Standard mode: backtick-quoted case-insensitive ---';

SELECT * FROM `testdbcase`.`testtablecase` ORDER BY Id;
SELECT * FROM `TESTDBCASE`.`TESTTABLECASE` ORDER BY Id;

-- Mix backtick and unquoted
SELECT * FROM `testdbcase`.testtablecase ORDER BY Id;
SELECT * FROM testdbcase.`testtablecase` ORDER BY Id;

SELECT '--- Ambiguity detection ---';

-- Create databases that differ only by case
DROP DATABASE IF EXISTS AmbigDB;
DROP DATABASE IF EXISTS AMBIGDB;
CREATE DATABASE AmbigDB;
CREATE DATABASE AMBIGDB;

CREATE TABLE AmbigDB.t (x UInt8) ENGINE = Memory;
INSERT INTO AmbigDB.t VALUES (1);
CREATE TABLE AMBIGDB.t (x UInt8) ENGINE = Memory;
INSERT INTO AMBIGDB.t VALUES (2);

SET case_insensitive_names = 'standard';
-- This should fail with AMBIGUOUS_IDENTIFIER
SELECT * FROM ambigdb.t; -- { serverError AMBIGUOUS_IDENTIFIER }

-- Double-quoted avoids ambiguity - selects from specific database
SELECT '--- Double-quoted avoids ambiguity ---';
SELECT * FROM "AmbigDB".t;
SELECT * FROM "AMBIGDB".t;

DROP DATABASE IF EXISTS AmbigDB;
DROP DATABASE IF EXISTS AMBIGDB;

SELECT '--- Table ambiguity detection ---';

DROP TABLE IF EXISTS AmbigTable;
DROP TABLE IF EXISTS AMBIGTABLE;
CREATE TABLE AmbigTable (x UInt8) ENGINE = Memory;
CREATE TABLE AMBIGTABLE (x UInt8) ENGINE = Memory;

SET case_insensitive_names = 'standard';
-- This should fail with AMBIGUOUS_IDENTIFIER
SELECT * FROM ambigtable; -- { serverError AMBIGUOUS_IDENTIFIER }

DROP TABLE IF EXISTS AmbigTable;
DROP TABLE IF EXISTS AMBIGTABLE;

DROP DATABASE IF EXISTS TestDBCase;

