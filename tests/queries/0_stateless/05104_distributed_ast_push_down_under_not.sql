-- Tags: shard
-- https://github.com/ClickHouse/ClickHouse/issues/113445
-- A predicate pushed to remote shards as an AST skips conjuncts it cannot convert (a lambda, or a
-- function marked non-deterministic). Skipping a conjunct weakens an `AND`, which is only sound with
-- positive polarity: under `NOT` it makes the pushed predicate stronger, so the shard dropped rows
-- the initiator-side filter can never bring back.

DROP TABLE IF EXISTS t_push_ast;
CREATE TABLE t_push_ast (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_push_ast SELECT number, number % 100 FROM numbers(1000);

SELECT count() FROM t_push_ast WHERE NOT (a < 500 AND arrayExists(x -> x < 86, [b]));
SELECT count() FROM (SELECT * FROM remote('127.0.0.2', currentDatabase(), t_push_ast))
WHERE NOT (a < 500 AND arrayExists(x -> x < 86, [b]));
SELECT count() FROM (SELECT * FROM remote('127.0.0.2', currentDatabase(), t_push_ast))
WHERE NOT (a < 500 AND arrayExists(x -> x < 86, [b]))
SETTINGS allow_push_predicate_ast_for_distributed_subqueries = 0;

SELECT 'a NOT over a single non-convertible conjunct';
SELECT count() FROM t_push_ast WHERE NOT arrayExists(x -> x < 86, [b]);
SELECT count() FROM (SELECT * FROM remote('127.0.0.2', currentDatabase(), t_push_ast))
WHERE NOT arrayExists(x -> x < 86, [b]);

SELECT 'a NOT over an OR';
SELECT count() FROM t_push_ast WHERE NOT (a < 500 OR arrayExists(x -> x < 86, [b]));
SELECT count() FROM (SELECT * FROM remote('127.0.0.2', currentDatabase(), t_push_ast))
WHERE NOT (a < 500 OR arrayExists(x -> x < 86, [b]));

SELECT 'a top-level AND still pushes its convertible conjuncts';
SELECT count() FROM t_push_ast WHERE a < 500 AND arrayExists(x -> x < 86, [b]);
SELECT count() FROM (SELECT * FROM remote('127.0.0.2', currentDatabase(), t_push_ast))
WHERE a < 500 AND arrayExists(x -> x < 86, [b]);
SELECT count() FROM (SELECT * FROM remote('127.0.0.2', currentDatabase(), t_push_ast))
WHERE a < 500 AND arrayExists(x -> x < 86, [b])
SETTINGS allow_push_predicate_ast_for_distributed_subqueries = 0;

SELECT 'a nested AND under a top-level AND';
SELECT count() FROM t_push_ast WHERE a < 900 AND (a > 100 AND arrayExists(x -> x < 86, [b]));
SELECT count() FROM (SELECT * FROM remote('127.0.0.2', currentDatabase(), t_push_ast))
WHERE a < 900 AND (a > 100 AND arrayExists(x -> x < 86, [b]));

SELECT 'fully convertible predicates are still pushed';
SELECT count() FROM (SELECT * FROM remote('127.0.0.2', currentDatabase(), t_push_ast)) WHERE NOT (a < 500 AND b >= 86);
SELECT count() FROM t_push_ast WHERE NOT (a < 500 AND b >= 86);

SELECT 'the shard still receives the filter for a top-level AND, and none under NOT';
SELECT count() FROM (SELECT * FROM remote('127.0.0.2', currentDatabase(), t_push_ast))
WHERE a < 500 AND arrayExists(x -> x < 86, [b]) SETTINGS log_comment = '05104_and' FORMAT Null;
SELECT count() FROM (SELECT * FROM remote('127.0.0.2', currentDatabase(), t_push_ast))
WHERE NOT (a < 500 AND arrayExists(x -> x < 86, [b])) SETTINGS log_comment = '05104_not' FORMAT Null;
SYSTEM FLUSH LOGS query_log;
SELECT log_comment, countIf(query LIKE '%HAVING%') > 0
FROM system.query_log
WHERE event_date >= yesterday() AND event_time > now() - 600 AND type = 'QueryFinish' AND is_initial_query = 0
    AND log_comment IN ('05104_and', '05104_not') AND query LIKE '%t_push_ast%'
GROUP BY log_comment ORDER BY log_comment;

DROP TABLE t_push_ast;
