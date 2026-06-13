-- Tags: no-random-settings, no-random-merge-tree-settings, no-parallel-replicas
-- no-random-settings, no-random-merge-tree-settings, no-parallel-replicas: Explain output may differ

-- { echo }

-- Regression for the in-order `LIMIT BY` streaming transform when the `LIMIT BY` keys cover the sort
-- prefix in a permuted order: keys `(a, b)` over `ORDER BY (b, a)`. The chunk is sorted by `(b, a)`, so
-- column `a` alone is NOT contiguous within it: a long `(a = 1, b = 1)` run, then one `(a = 2, b = 1)`,
-- then a long `(a = 1, b = 2)` run. `getEqualRangeEndAssumeSorted` must narrow keys in physical sort
-- order `(b, a)`; narrowing by `a` first (the clause order) would gallop past the `(2, 1)` row and merge
-- it into the `(1, 1)` group, dropping the `(2, 1)` group entirely. The single `(2, 1)` row and the long
-- equal runs (longer than the linear-probe window) are what force the galloping search to misbehave.
DROP TABLE IF EXISTS test_limit_by_overshoot;
CREATE TABLE test_limit_by_overshoot (a Int32, b Int32) ENGINE = MergeTree ORDER BY (b, a);
INSERT INTO test_limit_by_overshoot SELECT number < 100 ? 1 : (number = 100 ? 2 : 1) AS a, number <= 100 ? 1 : 2 AS b FROM numbers(201);
-- Reading in primary-key order makes the keys a permuted sort prefix: the streaming transform runs.
SELECT DISTINCT trim(BOTH ' ' FROM explain) FROM (EXPLAIN PIPELINE SELECT a, b FROM test_limit_by_overshoot LIMIT 1 BY a, b SETTINGS optimize_limit_by_in_order = 1, max_threads = 1) WHERE explain ILIKE '%LimitBy%Transform%';
-- One row per distinct `(a, b)` group; the `(2, 1)` group must not be dropped.
SELECT a, b FROM (SELECT a, b FROM test_limit_by_overshoot LIMIT 1 BY a, b SETTINGS optimize_limit_by_in_order = 1, max_threads = 1) ORDER BY a, b;
DROP TABLE test_limit_by_overshoot;
