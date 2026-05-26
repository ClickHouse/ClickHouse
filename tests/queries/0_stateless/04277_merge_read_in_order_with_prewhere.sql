-- Tags: no-random-merge-tree-settings

-- https://github.com/ClickHouse/ClickHouse/issues/86313
-- When reading from the `merge` table function with a WHERE clause that can be pushed
-- down to a prewhere on the underlying `MergeTree`, the optimizer used to fail to apply
-- the read-in-order optimization. After filter push-down, the inner plan injects a
-- `materialize(...)` wrapper into the prewhere output, which broke the sort-key match.

DROP TABLE IF EXISTS users_86313;

CREATE TABLE users_86313 (uid Int16, name String, age Int16, time Int8, PRIMARY KEY(time, age, name)) ENGINE = MergeTree;

INSERT INTO users_86313 VALUES (1231, 'Ksenia', 33, 2);
INSERT INTO users_86313 VALUES (6666, 'Ksenia', 48, 2);
INSERT INTO users_86313 VALUES (8888, 'John', 50, 2);

SET optimize_read_in_order=1;

-- Sanity check that the result is the same for both reads.
SELECT name FROM users_86313 WHERE time > 1 ORDER BY time, age LIMIT 1;
SELECT name FROM merge(currentDatabase(), '^users_86313$') WHERE time > 1 ORDER BY time, age LIMIT 1;

-- Both plans must use `ReadType: InOrder` for the underlying `MergeTree`.
SELECT count() FROM
(
    EXPLAIN PLAN actions = 1 SELECT name FROM users_86313 WHERE time > 1 ORDER BY time, age LIMIT 1
)
WHERE explain ILIKE '%ReadType: InOrder%';

SELECT count() FROM
(
    EXPLAIN PLAN actions = 1 SELECT name FROM merge(currentDatabase(), '^users_86313$') WHERE time > 1 ORDER BY time, age LIMIT 1
)
WHERE explain ILIKE '%ReadType: InOrder%';

DROP TABLE users_86313;
