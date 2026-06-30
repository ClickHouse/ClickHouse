-- Tags: no-random-settings, no-random-merge-tree-settings, no-parallel-replicas
-- no-random-settings, no-random-merge-tree-settings, no-parallel-replicas: Explain output may differ

-- { echo }

-- Negative `LIMIT BY` variant of the permuted-key galloping regression. With `ORDER BY (b, a)` the keys
-- `(a, b)` cover the sort prefix in permuted order, so the streaming `NegativeLimitBySortedStreamTransform`
-- runs. Within a chunk sorted by `(b, a)`, column `a` is not contiguous: a long `(a = 1, b = 1)` run, then
-- one `(a = 2, b = 1)`, then a long `(a = 1, b = 2)` run. Narrowing by `a` first (the clause order) would
-- gallop past the `(2, 1)` row and merge it into the `(1, 1)` group, dropping the `(2, 1)` group. The
-- transform must narrow keys in physical sort order `(b, a)`.
DROP TABLE IF EXISTS test_neg_limit_by_overshoot;
CREATE TABLE test_neg_limit_by_overshoot (a Int32, b Int32) ENGINE = MergeTree ORDER BY (b, a);
INSERT INTO test_neg_limit_by_overshoot SELECT number < 100 ? 1 : (number = 100 ? 2 : 1) AS a, number <= 100 ? 1 : 2 AS b FROM numbers(201);
-- `ORDER BY (b, a)` makes the keys a permuted sort prefix: the streaming transform runs.
SELECT DISTINCT trim(BOTH ' ' FROM explain) FROM (EXPLAIN PIPELINE SELECT a, b FROM test_neg_limit_by_overshoot ORDER BY b, a LIMIT -1 BY a, b SETTINGS max_threads = 1) WHERE explain ILIKE '%NegativeLimitBy%Transform%';
-- One row per distinct `(a, b)` group; the `(2, 1)` group must not be dropped.
SELECT a, b FROM (SELECT a, b FROM test_neg_limit_by_overshoot ORDER BY b, a LIMIT -1 BY a, b SETTINGS max_threads = 1) ORDER BY a, b;
DROP TABLE test_neg_limit_by_overshoot;
