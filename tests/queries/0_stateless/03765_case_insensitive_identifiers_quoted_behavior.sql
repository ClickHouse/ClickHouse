-- Tags: no-parallel
-- no-parallel - when executing in parallel, we might have table/db name collisions

SET allow_experimental_analyzer = 1;

DROP DATABASE IF EXISTS test_03765_q SYNC;

CREATE DATABASE test_03765_q ENGINE = Atomic;
CREATE TABLE test_03765_q.Qt (A UInt8) ENGINE = Memory;
INSERT INTO test_03765_q.Qt VALUES (1);

-- DB CI ON, tables CI ON
SET enable_case_insensitive_databases = 1;
SET enable_case_insensitive_tables = 1;
SET enable_case_insensitive_columns = 0;
SELECT 'q-ok-1';

-- quoted DB wrong-case, exact-case and wrong-case table names should resolve when CI is ON
SELECT sum(A) FROM `test_03765_Q`.Qt; -- 1
SELECT sum(A) FROM `test_03765_Q`.`qt`; -- 1

-- unquoted DB wrong-case, quoted table exact-case
SELECT sum(A) FROM test_03765_Q.`Qt`; -- 1

-- column CI ON also applies to quoted column
SET enable_case_insensitive_columns = 1;
SELECT A FROM test_03765_Q.Qt; -- 1
SELECT `a` FROM test_03765_Q.Qt; -- 1

DROP DATABASE IF EXISTS test_03765_q SYNC;
