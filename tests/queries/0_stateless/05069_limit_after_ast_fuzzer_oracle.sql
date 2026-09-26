-- The AST fuzzer's oracles treat a `LIMIT AFTER`/`UNTIL` range like `LIMIT`: its rows depend on the order
-- of the result, so partitioning the query by `WHERE` and comparing row sets is not sound and the oracles
-- skip such queries instead of reporting a mismatch.
SET ast_fuzzer_runs = 3;
SET ast_fuzzer_oracle = 1;
-- The random mutations also produce queries that fail, and the fuzzer logs every such failure at the
-- error level; the test checks only the results, so those log lines must not reach the client.
SET send_logs_level = 'fatal';

DROP TABLE IF EXISTS t_oracle_range;
CREATE TABLE t_oracle_range (n UInt8, p UInt8) ENGINE = Memory;
INSERT INTO t_oracle_range SELECT number, number % 2 FROM numbers(10);

SELECT n FROM t_oracle_range WHERE n % 2 = 0 ORDER BY n LIMIT AFTER n = 4;
SELECT n FROM t_oracle_range WHERE p ORDER BY n LIMIT UNTIL n = 5;
SELECT n FROM t_oracle_range WHERE n % 3 = 0 ORDER BY n LIMIT 1 AFTER n IN (3, 6) ALL;

DROP TABLE t_oracle_range;
