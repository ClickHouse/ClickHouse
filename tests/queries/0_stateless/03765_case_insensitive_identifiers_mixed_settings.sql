-- Tags: no-parallel
-- no-parallel - when executing in parallel, we might have table/db name collisions

SET allow_experimental_analyzer = 1;

DROP DATABASE IF EXISTS test_03765_miX SYNC;

CREATE DATABASE test_03765_miX ENGINE = Atomic;
CREATE TABLE test_03765_miX.T (A UInt8, b UInt8) ENGINE = Memory;
INSERT INTO test_03765_miX.T VALUES (1, 2);

-- DB CI ON, table CI OFF, column CI OFF
SET enable_case_insensitive_databases = 1;
SET enable_case_insensitive_tables = 0;
SET enable_case_insensitive_columns = 0;
SELECT 'ok-mix-1';
SELECT sum(A) FROM test_03765_mix.t; -- { serverError UNKNOWN_TABLE }
SELECT sum(A) FROM test_03765_mix.T; -- 1

-- flip tables CI ON in-place
SET enable_case_insensitive_tables = 1;
SELECT sum(A) FROM test_03765_mix.t; -- 1

-- flip DB CI OFF (tables CI ON) -> wrong-case DB fails
SET enable_case_insensitive_databases = 0;
SELECT sum(A) FROM test_03765_mix.t; -- { serverError UNKNOWN_DATABASE }
SELECT sum(A) FROM test_03765_miX.T; -- 1

-- columns CI ON: wrong-case column resolves
SET enable_case_insensitive_columns = 1;
SELECT sum(a) FROM test_03765_miX.T; -- 1

-- columns CI OFF: wrong-case column unknown
SET enable_case_insensitive_columns = 0;
SELECT sum(a) FROM test_03765_miX.T; -- { serverError UNKNOWN_IDENTIFIER }

-- mix: DB CI ON, tables CI OFF, columns CI ON
SET enable_case_insensitive_databases = 1;
SET enable_case_insensitive_tables = 0;
SET enable_case_insensitive_columns = 1;
SELECT 'ok-mix-2';
SELECT sum(a) FROM test_03765_mix.t; -- { serverError UNKNOWN_TABLE }
SELECT sum(a) FROM test_03765_mix.T; -- 1

-- quoted DB ignores tables setting (DB CI ON still fixes DB)
SELECT sum(A) FROM `test_03765_mix`.t; -- { serverError UNKNOWN_TABLE }

-- flip tables CI ON now, quoted DB should still work, wrong-case table resolved
SET enable_case_insensitive_tables = 1;
SELECT sum(A) FROM `test_03765_mix`.t; -- 1

-- flip everything OFF -> only exact case works
SET enable_case_insensitive_databases = 0;
SET enable_case_insensitive_tables = 0;
SET enable_case_insensitive_columns = 0;
SELECT sum(A) FROM test_03765_miX.T; -- 1
SELECT sum(a) FROM test_03765_miX.T; -- { serverError UNKNOWN_IDENTIFIER }
SELECT sum(A) FROM test_03765_miX.t; -- { serverError UNKNOWN_TABLE }

DROP DATABASE IF EXISTS test_03765_mix SYNC;
