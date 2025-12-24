-- Tags: no-parallel
-- no-parallel - when executing in parallel, we might have table/db name collisions

SET allow_experimental_analyzer = 1;

DROP DATABASE IF EXISTS test_03765_tblpick SYNC;

-- single DB, table name presence disambiguates wrong-case reference under CI
CREATE DATABASE test_03765_tblpick ENGINE = Atomic;
CREATE TABLE test_03765_tblpick.Ta (x UInt8) ENGINE = Memory;

SET enable_case_insensitive_tables = 1;
-- only one table that matches lower('ta') exists -> resolves uniquely
SELECT count() FROM test_03765_tblpick.ta; -- 0

-- add another table that collides by lower -> becomes ambiguous
CREATE TABLE test_03765_tblpick.tA (x UInt8) ENGINE = Memory;
SELECT count() FROM test_03765_tblpick.ta; -- { serverError AMBIGUOUS_IDENTIFIER }

-- exact-case remains unambiguous
SELECT count() FROM test_03765_tblpick.Ta; -- 0

DROP DATABASE IF EXISTS test_03765_tblpick SYNC;

DROP DATABASE IF EXISTS test_03765_pick SYNC;
DROP DATABASE IF EXISTS tEsT_03765_pIcK SYNC;

CREATE DATABASE test_03765_pick ENGINE = Atomic;
CREATE DATABASE tEsT_03765_pIcK ENGINE = Atomic;

-- create table only in the first DB
CREATE TABLE test_03765_pick.T (x UInt8) ENGINE = Memory;

-- DB CI ON, table CI OFF: exact-case table disambiguates by presence
SET enable_case_insensitive_databases = 1;
SET enable_case_insensitive_tables = 0;
SELECT count() FROM test_03765_pick.T; -- 0

-- DB CI ON, table CI ON: wrong-case table also resolves uniquely by presence
SET enable_case_insensitive_tables = 1;
SELECT count() FROM test_03765_pick.t; -- 0

-- add same table to the second DB -> ambiguity appears for wrong-case table
CREATE TABLE tEsT_03765_pIcK.T (x UInt8) ENGINE = Memory;
SELECT count() FROM test_03765_pick.t; -- { serverError AMBIGUOUS_IDENTIFIER }

-- exact-case remains unambiguous
SELECT count() FROM test_03765_pick.T; -- 0

DROP DATABASE IF EXISTS test_03765_pick SYNC;
DROP DATABASE IF EXISTS tEsT_03765_pIcK SYNC;

DROP DATABASE IF EXISTS test_03765_colpick SYNC;

-- two joined tables, only left has a column that matches 'a' case-insensitively
CREATE DATABASE test_03765_colpick ENGINE = Atomic;
CREATE TABLE test_03765_colpick.t1 (id UInt8, A UInt8) ENGINE = Memory;
CREATE TABLE test_03765_colpick.t2 (id UInt8, b UInt8) ENGINE = Memory;
INSERT INTO test_03765_colpick.t1 VALUES (1, 10);
INSERT INTO test_03765_colpick.t2 VALUES (1, 20);

-- CI columns ON, unqualified wrong-case 'a' should resolve uniquely to t1
SET enable_case_insensitive_columns = 1;
SELECT sum(a) FROM test_03765_colpick.t1 AS x INNER JOIN test_03765_colpick.t2 AS y ON x.id = y.id; -- 10

-- add a case-insensitive match to the right table -> becomes ambiguous
SET single_join_prefer_left_table = 0;
ALTER TABLE test_03765_colpick.t2 ADD COLUMN a UInt8 DEFAULT 20;
SELECT sum(a) FROM test_03765_colpick.t1 AS x INNER JOIN test_03765_colpick.t2 AS y ON x.id = y.id; -- { serverError AMBIGUOUS_IDENTIFIER }

-- turn CI OFF: wrong-case should be unknown, exact-case still works
SET enable_case_insensitive_columns = 0;
SELECT sum(aa) FROM test_03765_colpick.t1 AS x INNER JOIN test_03765_colpick.t2 AS y ON x.id = y.id; -- { serverError UNKNOWN_IDENTIFIER }
SELECT sum(A) FROM test_03765_colpick.t1 AS x INNER JOIN test_03765_colpick.t2 AS y ON x.id = y.id; -- 10

DROP DATABASE IF EXISTS test_03765_colpick SYNC;
