-- Tags: shard
-- A pushed down filter on an expression column must not exceed the AST limits of the shards.

DROP TABLE IF EXISTS t;
CREATE TABLE t (number UInt64, k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t SELECT number, number + 1 FROM numbers(3);

SELECT count() FROM (SELECT k, (number + 1) * (number + 2) * (number + 3) * (number + 4) * (number + 5) AS x
    FROM remote('127.0.0.{2,3}', currentDatabase(), t) ORDER BY k)
WHERE k > 1 AND x != 1 AND x > 2 AND x % 2 = 0 AND x % 3 = 0 AND x % 4 = 0 AND x % 5 = 0 AND x % 6 = 0 AND x % 8 = 0
SETTINGS max_ast_elements = 200, allow_push_predicate_ast_for_distributed_subqueries = 1;

SELECT number FROM (SELECT k, k + 1 + 1 + 1 + 1 + 1 + 1 + 1 + 1 AS number
    FROM remote('127.0.0.{2,3}', currentDatabase(), t) ORDER BY k)
WHERE number + 1 + 1 + 1 + 1 + 1 + 1 + 1 + 1 = 19
SETTINGS max_ast_depth = 32, allow_push_predicate_ast_for_distributed_subqueries = 1;

DROP TABLE t;
