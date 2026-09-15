SET enable_parallel_replicas = 0;
SET optimize_read_in_order = 1;
DROP TABLE IF EXISTS t_row_policy_rio;

CREATE TABLE t_row_policy_rio
(
    key1 UInt64,
    key2 UInt64
)
ENGINE = MergeTree
ORDER BY (key1, key2);

INSERT INTO t_row_policy_rio
SELECT c1 % 10, c2
FROM generateRandom('c1 UInt64, c2 UInt64', 42)
LIMIT 1000;

CREATE ROW POLICY test_policy ON t_row_policy_rio USING key1 = 0 TO ALL;

SELECT '-- Row policy only (no explicit key1 in WHERE)';

SELECT trim(explain) FROM (
EXPLAIN actions = 1, indexes = 1
SELECT key2 FROM t_row_policy_rio
ORDER BY key2
LIMIT 100
) WHERE explain LIKE '%ReadType%';

SELECT '-- Explicit key1 in WHERE clause';

SELECT trim(explain) FROM (
EXPLAIN actions = 1, indexes = 1
SELECT key2 FROM t_row_policy_rio
WHERE key1 = 0
ORDER BY key2
LIMIT 100
) WHERE explain LIKE '%ReadType%';

DROP ROW POLICY test_policy ON t_row_policy_rio;
DROP TABLE t_row_policy_rio;