-- Tags: no-fasttest
-- Test: exercises `Storage FuzzQuery` engine creation paths registered by `registerStorageFuzzQuery`.
-- Covers: src/Storages/StorageFuzzQuery.cpp:166-167 (empty-args), :171-173 (non-String column),
--         and src/Storages/StorageFuzzQuery.cpp:131 (too many args via `getConfiguration`).
-- The PR's own test 03031 only uses the `fuzzQuery(...)` table function form;
-- `CREATE TABLE ... ENGINE = FuzzQuery(...)` registration paths are unexercised.

SET allow_fuzz_query_functions = 1;

DROP TABLE IF EXISTS t_fq_engine;

-- Happy path: storage engine accepts (query, max_query_length, random_seed) and produces rows.
CREATE TABLE t_fq_engine (q String) ENGINE = FuzzQuery('SELECT 1', 500, 8956);
SELECT count() FROM (SELECT * FROM t_fq_engine LIMIT 3);
DROP TABLE t_fq_engine;

-- Error path 1: non-String column type — exercises `BAD_ARGUMENTS` branch in the registration lambda.
CREATE TABLE t_fq_engine (q UInt64) ENGINE = FuzzQuery('SELECT 1', 500, 8956); -- { serverError BAD_ARGUMENTS }

-- Error path 2: empty args — exercises `engine_args.empty()` branch in the registration lambda.
CREATE TABLE t_fq_engine (q String) ENGINE = FuzzQuery(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

-- Error path 3: too many args — exercises the `engine_args.size() > 3` branch in `getConfiguration`.
CREATE TABLE t_fq_engine (q String) ENGINE = FuzzQuery('SELECT 1', 500, 8956, 9999); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

DROP TABLE IF EXISTS t_fq_engine;
