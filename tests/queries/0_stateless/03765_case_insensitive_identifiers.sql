SET allow_experimental_analyzer = 1;

DROP DATABASE IF EXISTS test_03765_DbAmbA SYNC;
DROP DATABASE IF EXISTS test_03765_dBaMbA SYNC;
DROP DATABASE IF EXISTS test_03765_CaseDB SYNC;
DROP DATABASE IF EXISTS test_03765_tbl_db SYNC;
DROP DATABASE IF EXISTS test_03765_col_db SYNC;

CREATE DATABASE test_03765_CaseDB ENGINE = Atomic;
CREATE TABLE test_03765_CaseDB.T1 (x UInt8) ENGINE = Memory;
INSERT INTO test_03765_CaseDB.T1 VALUES (1);

-- DB CI ON, table CI OFF: wrong-case DB, exact table -> OK
SET enable_case_insensitive_databases = 1;
SET enable_case_insensitive_tables = 0;
SELECT 'ok-1';
SELECT sum(x) FROM test_03765_casedb.T1; -- 1

-- DB CI ON, table CI OFF: wrong-case DB and table -> UNKNOWN_TABLE
SELECT sum(x) FROM test_03765_casedb.t1; -- { serverError UNKNOWN_TABLE }

-- DB CI ON, table CI ON: wrong-case DB and table -> OK
SET enable_case_insensitive_tables = 1;
SELECT sum(x) FROM test_03765_casedb.t1; -- 1

-- DB CI OFF, table CI ON: wrong-case DB -> UNKNOWN_DATABASE
SET enable_case_insensitive_databases = 0;
SET enable_case_insensitive_tables = 1;
SELECT sum(x) FROM test_03765_casedb.t1; -- { serverError UNKNOWN_DATABASE }
SELECT sum(x) FROM test_03765_CaseDB.t1; -- 1

-- database ambiguity (two DBs with same ASCII-lower)
CREATE DATABASE test_03765_DbAmbA ENGINE = Atomic;
CREATE DATABASE test_03765_dBaMbA ENGINE = Atomic;
SET enable_case_insensitive_databases = 1;
SELECT 1 FROM test_03765_dBAmBa.nonexistent; -- { serverError AMBIGUOUS_IDENTIFIER }
SET enable_case_insensitive_databases = 0;
SELECT 1 FROM test_03765_dBAmBa.nonexistent; -- { serverError UNKNOWN_DATABASE }

-- 1-part table identifier fallback within current DB
USE test_03765_CaseDB;
SET enable_case_insensitive_tables = 0;
SELECT 'ok-2';
SELECT sum(x) FROM t1; -- { serverError UNKNOWN_TABLE }
SET enable_case_insensitive_tables = 1;
SELECT sum(x) FROM t1; -- 1
USE default;

-- table ambiguity within a single DB (no exact case match for query)
CREATE DATABASE test_03765_tbl_db ENGINE = Atomic;
CREATE TABLE test_03765_tbl_db.Foo (x UInt8) ENGINE = Memory;
CREATE TABLE test_03765_tbl_db.FOO (x UInt8) ENGINE = Memory;
SET enable_case_insensitive_tables = 1;
SELECT 1 FROM test_03765_tbl_db.fOo; -- { serverError AMBIGUOUS_IDENTIFIER }
SET enable_case_insensitive_tables = 0;
SELECT 1 FROM test_03765_tbl_db.fOo; -- { serverError UNKNOWN_TABLE }

-- exact match short-circuits before CI scanning
SET enable_case_insensitive_tables = 1;
CREATE TABLE test_03765_tbl_db.BaR (x UInt8) ENGINE = Memory;
SELECT 'ok-3';
SELECT count() FROM test_03765_tbl_db.BaR; -- 0

-- column CI: ON resolves wrong-case; OFF fails
CREATE DATABASE test_03765_col_db ENGINE = Atomic;
CREATE TABLE test_03765_col_db.ct (Foo UInt8, Bar UInt8) ENGINE = Memory;
INSERT INTO test_03765_col_db.ct VALUES (1, 2);
SET enable_case_insensitive_columns = 0;
SELECT 'ok-4';
SELECT Foo FROM test_03765_col_db.ct; -- 1
SELECT foo FROM test_03765_col_db.ct; -- { serverError UNKNOWN_IDENTIFIER }
SET enable_case_insensitive_columns = 1;
SELECT foo FROM test_03765_col_db.ct; -- 1

-- column CI ambiguity (two columns with same ASCII-lower, no exact for query)
CREATE TABLE test_03765_col_db.ct2 (Score Int32, sCOrE Int32) ENGINE = Memory;
SET enable_case_insensitive_columns = 1;
-- no rows yet; just exercise resolution path
SELECT Score FROM test_03765_col_db.ct2; -- (no output)
SELECT score FROM test_03765_col_db.ct2; -- { serverError AMBIGUOUS_IDENTIFIER }
SET enable_case_insensitive_columns = 0;
SELECT score FROM test_03765_col_db.ct2; -- { serverError UNKNOWN_IDENTIFIER }

-- joins with CI column fallback
CREATE TABLE test_03765_col_db.j1 (ID UInt8, v UInt8) ENGINE = Memory;
CREATE TABLE test_03765_col_db.j2 (id UInt8, v UInt8) ENGINE = Memory;
INSERT INTO test_03765_col_db.j1 VALUES (1, 10);
INSERT INTO test_03765_col_db.j2 VALUES (1, 20);
SET enable_case_insensitive_columns = 1;
SELECT sum(j1.v + j2.v) FROM test_03765_col_db.j1 AS j1 INNER JOIN test_03765_col_db.j2 AS j2 ON j1.id = j2.id; -- 30
SELECT sum(j1.v) FROM test_03765_col_db.j1 AS j1 WHERE j1.id = 1; -- 10
SET enable_case_insensitive_columns = 0;
-- mismatch case on j1 side (column is ID) -> should fail when CI is OFF
SELECT sum(j1.v + j2.v) FROM test_03765_col_db.j1 AS j1 INNER JOIN test_03765_col_db.j2 AS j2 ON j1.id = j2.id; -- { serverError UNKNOWN_IDENTIFIER }
-- also verify WHERE path fails without CI
SELECT sum(j1.v) FROM test_03765_col_db.j1 AS j1 WHERE j1.id = 1; -- { serverError UNKNOWN_IDENTIFIER }

DROP DATABASE IF EXISTS test_03765_DbAmbA SYNC;
DROP DATABASE IF EXISTS test_03765_dBaMbA SYNC;
DROP DATABASE IF EXISTS test_03765_tbl_db SYNC;
DROP DATABASE IF EXISTS test_03765_col_db SYNC;
DROP DATABASE IF EXISTS test_03765_CaseDB SYNC;

DROP DATABASE IF EXISTS test_03361_DbAmb SYNC;
DROP DATABASE IF EXISTS test_03361_dBAmB SYNC;

CREATE DATABASE test_03361_DbAmb ENGINE = Atomic;
CREATE DATABASE test_03361_dBAmB ENGINE = Atomic;

-- CI databases ON: ambiguous DB name should error
SET enable_case_insensitive_databases = 1;
SELECT 1 FROM test_03361_dbamb.nonexistent; -- { serverError AMBIGUOUS_IDENTIFIER }

-- CI databases OFF: unknown database
SET enable_case_insensitive_databases = 0;
SELECT 1 FROM test_03361_dbamb.nonexistent; -- { serverError UNKNOWN_DATABASE }

-- Quoted DB name still participates in CI database matching
SET enable_case_insensitive_databases = 1;
SELECT 1 FROM `test_03361_dbamb`.nonexistent; -- { serverError AMBIGUOUS_IDENTIFIER }

DROP DATABASE IF EXISTS test_03361_DbAmb SYNC;
DROP DATABASE IF EXISTS test_03361_dBAmB SYNC;
