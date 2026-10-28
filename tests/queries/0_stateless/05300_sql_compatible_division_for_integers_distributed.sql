-- Tags: distributed

SET prefer_localhost_replica = 0;
SET enable_parallel_replicas = 0;
SET automatic_parallel_replicas_mode = 0;

CREATE TABLE exact_division_distributed (x Int64) ENGINE = MergeTree ORDER BY x;
INSERT INTO exact_division_distributed VALUES (-7), (1), (1), (7);

SET sql_compatible_division_for_integers = 1;
SELECT x / 2 AS q, toTypeName(q) FROM remote('127.0.0.1', currentDatabase(), exact_division_distributed)
ORDER BY x SETTINGS serialize_query_plan = 0;
SELECT x / 2 AS q, toTypeName(q) FROM remote('127.0.0.1', currentDatabase(), exact_division_distributed)
ORDER BY x SETTINGS serialize_query_plan = 1;
SELECT sum(x / 2) FROM remote('127.0.0.1', currentDatabase(), exact_division_distributed)
SETTINGS serialize_query_plan = 1, optimize_arithmetic_operations_in_aggregate_functions = 1;

SET sql_compatible_division_for_integers = 0;
SELECT x / 2 AS q, toTypeName(q) FROM remote('127.0.0.1', currentDatabase(), exact_division_distributed)
ORDER BY x SETTINGS serialize_query_plan = 1;
DROP TABLE exact_division_distributed;
