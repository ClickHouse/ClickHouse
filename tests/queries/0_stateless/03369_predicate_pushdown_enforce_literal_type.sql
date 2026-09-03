DROP TABLE IF EXISTS t_03369;

CREATE TABLE t_03369 (d Date, event String, c UInt32) ENGINE = Memory;

INSERT INTO t_03369 VALUES (toDate('2025-03-03'), 'foo', 1);

SET prefer_localhost_replica = 0, allow_push_predicate_ast_for_distributed_subqueries = 1;

SELECT d, count() FROM remote('127.0.0.{1..10}', currentDatabase(), t_03369) WHERE event != '' GROUP BY d HAVING d >= toDate(1738281600) AND count() >= 1;
