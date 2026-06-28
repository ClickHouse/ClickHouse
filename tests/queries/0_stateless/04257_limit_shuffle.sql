SELECT number FROM numbers(10) LIMIT 1 SHUFFLE; -- { serverError SUPPORT_IS_DISABLED }
SELECT number FROM numbers(10) LIMIT 1 SHUFFLE SETTINGS allow_experimental_shuffle_query = 1, enable_analyzer = 0; -- { serverError SUPPORT_IS_DISABLED }
SELECT number FROM numbers(10) LIMIT 1 SHUFFLE SETTINGS allow_experimental_shuffle_query = 1, enable_analyzer = 1, use_query_cache = 1; -- { serverError QUERY_CACHE_USED_WITH_NONDETERMINISTIC_FUNCTIONS }
CREATE TEMPORARY TABLE limit_shuffle_insert_sink (number UInt64);
INSERT INTO limit_shuffle_insert_sink SELECT number FROM numbers(10) LIMIT 1 SHUFFLE SETTINGS allow_experimental_shuffle_query = 1, enable_analyzer = 0; -- { serverError SUPPORT_IS_DISABLED }
CREATE TEMPORARY TABLE limit_shuffle_create_sink ENGINE = Memory AS SELECT number FROM numbers(10) LIMIT 1 SHUFFLE SETTINGS allow_experimental_shuffle_query = 1, enable_analyzer = 0; -- { serverError SUPPORT_IS_DISABLED }
CREATE TABLE limit_shuffle_mv_src (number UInt64, value UInt64) ENGINE = MergeTree ORDER BY number;
CREATE MATERIALIZED VIEW limit_shuffle_mv ENGINE = Memory AS SELECT number, value FROM limit_shuffle_mv_src;
SET enable_analyzer = 1;
ALTER TABLE limit_shuffle_mv MODIFY QUERY SELECT number, value FROM limit_shuffle_mv_src LIMIT 1 SHUFFLE SETTINGS allow_experimental_shuffle_query = 1;
SELECT * FROM limit_shuffle_mv SETTINGS use_query_cache = 1;
SET enable_analyzer = 0;
INSERT INTO limit_shuffle_mv_src SETTINGS enable_analyzer = 0 VALUES (1, 1); -- { serverError SUPPORT_IS_DISABLED }
DROP TABLE limit_shuffle_mv;
DROP TABLE limit_shuffle_mv_src;
SET enable_analyzer = 1;
CREATE VIEW limit_shuffle_parameterized_view_disabled AS SELECT number FROM numbers({n:UInt64}) LIMIT 1 SHUFFLE; -- { serverError SUPPORT_IS_DISABLED }
CREATE VIEW limit_shuffle_view AS SELECT number FROM numbers(10) LIMIT 1 SHUFFLE SETTINGS allow_experimental_shuffle_query = 1;
CREATE VIEW limit_shuffle_nested_view AS SELECT * FROM limit_shuffle_view;
CREATE VIEW limit_shuffle_parameterized_view AS SELECT number FROM numbers({n:UInt64}) LIMIT 1 SHUFFLE SETTINGS allow_experimental_shuffle_query = 1;
CREATE VIEW abs AS SELECT number FROM numbers({n:UInt64}) LIMIT 1 SHUFFLE SETTINGS allow_experimental_shuffle_query = 1;
CREATE VIEW numbers AS SELECT number FROM system.numbers LIMIT {n:UInt64} SHUFFLE SETTINGS allow_experimental_shuffle_query = 1;
CREATE TEMPORARY VIEW limit_shuffle_temporary_view AS SELECT number FROM numbers(10) LIMIT 1 SHUFFLE SETTINGS allow_experimental_shuffle_query = 1;
CREATE TABLE limit_shuffle_merge_view (number UInt64) ENGINE = Merge(currentDatabase(), '^limit_shuffle_view$');
CREATE TABLE limit_shuffle_alias_view ENGINE = Alias('limit_shuffle_view');
CREATE TABLE limit_shuffle_distributed_view (number UInt64) ENGINE = Distributed(test_shard_localhost, currentDatabase(), 'limit_shuffle_view');
CREATE TABLE limit_shuffle_plain_local (number UInt64) ENGINE = Memory;
CREATE TABLE limit_shuffle_plain_distributed (number UInt64) ENGINE = Distributed(test_shard_localhost, currentDatabase(), 'limit_shuffle_plain_local');
SELECT * FROM limit_shuffle_view SETTINGS enable_analyzer = 1, use_query_cache = 1; -- { serverError QUERY_CACHE_USED_WITH_NONDETERMINISTIC_FUNCTIONS }
SELECT * FROM limit_shuffle_nested_view SETTINGS enable_analyzer = 1, use_query_cache = 1; -- { serverError QUERY_CACHE_USED_WITH_NONDETERMINISTIC_FUNCTIONS }
SELECT * FROM limit_shuffle_parameterized_view(n = 10) SETTINGS enable_analyzer = 1, use_query_cache = 1; -- { serverError QUERY_CACHE_USED_WITH_NONDETERMINISTIC_FUNCTIONS }
SELECT * FROM abs(n = 10) SETTINGS enable_analyzer = 1, use_query_cache = 1; -- { serverError QUERY_CACHE_USED_WITH_NONDETERMINISTIC_FUNCTIONS }
SELECT * FROM numbers(3) SETTINGS enable_analyzer = 1, use_query_cache = 1 FORMAT Null;
SELECT * FROM merge(currentDatabase(), '^limit_shuffle_view$') SETTINGS enable_analyzer = 1, use_query_cache = 1; -- { serverError QUERY_CACHE_USED_WITH_NONDETERMINISTIC_FUNCTIONS }
SELECT * FROM cluster('test_shard_localhost', currentDatabase(), 'limit_shuffle_view') SETTINGS enable_analyzer = 1, use_query_cache = 1; -- { serverError QUERY_CACHE_USED_WITH_NONDETERMINISTIC_FUNCTIONS }
SELECT * FROM cluster('test_shard_localhost', merge(currentDatabase(), '^limit_shuffle_view$')) SETTINGS enable_analyzer = 1, use_query_cache = 1; -- { serverError QUERY_CACHE_USED_WITH_NONDETERMINISTIC_FUNCTIONS }
SELECT * FROM cluster('test_shard_localhost', merge(currentDatabase(), '^limit_shuffle_remote_only_view$')) SETTINGS enable_analyzer = 1, use_query_cache = 1; -- { serverError QUERY_CACHE_USED_WITH_NONDETERMINISTIC_FUNCTIONS }
SELECT * FROM remote('127.0.0.1', currentDatabase(), 'limit_shuffle_view') SETTINGS enable_analyzer = 1, use_query_cache = 1; -- { serverError QUERY_CACHE_USED_WITH_NONDETERMINISTIC_FUNCTIONS }
SELECT * FROM loop(limit_shuffle_view) LIMIT 1 SETTINGS enable_analyzer = 1, use_query_cache = 1; -- { serverError QUERY_CACHE_USED_WITH_NONDETERMINISTIC_FUNCTIONS }
SELECT * FROM loop(merge(currentDatabase(), '^limit_shuffle_view$')) LIMIT 1 SETTINGS enable_analyzer = 1, use_query_cache = 1; -- { serverError QUERY_CACHE_USED_WITH_NONDETERMINISTIC_FUNCTIONS }
SELECT * FROM limit_shuffle_merge_view SETTINGS enable_analyzer = 1, use_query_cache = 1; -- { serverError QUERY_CACHE_USED_WITH_NONDETERMINISTIC_FUNCTIONS }
SELECT * FROM limit_shuffle_alias_view SETTINGS enable_analyzer = 1, use_query_cache = 1; -- { serverError QUERY_CACHE_USED_WITH_NONDETERMINISTIC_FUNCTIONS }
SELECT * FROM limit_shuffle_distributed_view SETTINGS enable_analyzer = 1, use_query_cache = 1; -- { serverError QUERY_CACHE_USED_WITH_NONDETERMINISTIC_FUNCTIONS }
SELECT * FROM limit_shuffle_plain_distributed SETTINGS enable_analyzer = 1, use_query_cache = 1 FORMAT Null;
SELECT * FROM cluster('test_shard_localhost', currentDatabase(), 'limit_shuffle_plain_local') SETTINGS enable_analyzer = 1, use_query_cache = 1 FORMAT Null;
SELECT * FROM remote('127.0.0.1', currentDatabase(), 'limit_shuffle_plain_local') SETTINGS enable_analyzer = 1, use_query_cache = 1 FORMAT Null;
SELECT * FROM cluster('test_shard_localhost', numbers(2)) SETTINGS enable_analyzer = 1, use_query_cache = 1 FORMAT Null;
SELECT * FROM remote('127.0.0.1', numbers_mt(2)) SETTINGS enable_analyzer = 1, use_query_cache = 1 FORMAT Null;
SELECT * FROM cluster('test_shard_localhost', view(SELECT number FROM numbers(2))) SETTINGS enable_analyzer = 1, use_query_cache = 1 FORMAT Null;
SELECT * FROM cluster('test_shard_localhost', view(SELECT number FROM numbers(10) LIMIT 1 SHUFFLE SETTINGS allow_experimental_shuffle_query = 1)) SETTINGS enable_analyzer = 1, use_query_cache = 1; -- { serverError QUERY_CACHE_USED_WITH_NONDETERMINISTIC_FUNCTIONS }
SELECT * FROM cluster('test_shard_localhost', view(SELECT * FROM limit_shuffle_remote_only_view)) SETTINGS enable_analyzer = 1, use_query_cache = 1; -- { serverError QUERY_CACHE_USED_WITH_NONDETERMINISTIC_FUNCTIONS }
SELECT * FROM limit_shuffle_temporary_view SETTINGS enable_analyzer = 1, use_query_cache = 1; -- { serverError QUERY_CACHE_USED_WITH_NONDETERMINISTIC_FUNCTIONS }
SELECT * FROM limit_shuffle_view SETTINGS enable_analyzer = 0; -- { serverError SUPPORT_IS_DISABLED }
DROP TABLE limit_shuffle_plain_distributed;
DROP TABLE limit_shuffle_plain_local;
DROP TABLE limit_shuffle_distributed_view;
DROP TABLE limit_shuffle_alias_view;
DROP TABLE limit_shuffle_merge_view;
DROP VIEW limit_shuffle_temporary_view;
DROP VIEW numbers;
DROP VIEW abs;
DROP VIEW limit_shuffle_parameterized_view;
DROP VIEW limit_shuffle_nested_view;
DROP VIEW limit_shuffle_view;
SET allow_experimental_shuffle_query = 1;
SET enable_analyzer = 0;
EXPLAIN AST SELECT number FROM numbers(10) LIMIT 1 SHUFFLE;
EXPLAIN SELECT number FROM numbers(10) LIMIT 1 SHUFFLE; -- { serverError SUPPORT_IS_DISABLED }

SET allow_experimental_shuffle_query = 1;
SET enable_analyzer = 1;

EXPLAIN SYNTAX
SELECT number
FROM numbers(10)
LIMIT 3 SHUFFLE;

EXPLAIN header = 0
SELECT number
FROM numbers(10)
LIMIT 3 SHUFFLE;

SELECT count()
FROM
(
    SELECT number
    FROM numbers(100)
    LIMIT 7 SHUFFLE
);

SELECT number
FROM numbers(10) AS SHUFFLE
LIMIT 1;

SELECT number
FROM numbers(10)
LIMIT 1 SHUFFLE
SETTINGS allow_experimental_shuffle_query = 0; -- { serverError SUPPORT_IS_DISABLED }

SELECT number FROM numbers(10) ORDER BY number LIMIT 1 SHUFFLE; -- { clientError SYNTAX_ERROR }
SELECT number FROM numbers(10) LIMIT 1 OFFSET 1 SHUFFLE; -- { clientError SYNTAX_ERROR }
SELECT number FROM numbers(10) LIMIT 1 WITH TIES SHUFFLE; -- { clientError SYNTAX_ERROR }
