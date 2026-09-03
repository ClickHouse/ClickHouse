-- Regression for the keyless grouping set under `group_by_each_block_no_merge` (PR #54907, issue #9604).
-- `group_by_each_block_no_merge` flushes a `GROUP BY` result per input block without merging. The empty
-- grouping set `()` has no keys, so it has a single group and must still return one fully-merged grand-total
-- row, exactly as if the setting were off. `AggregatingStep` clones the aggregation params for every grouping
-- set via `Aggregator::Params::cloneWithKeys`; before the fix that clone kept the no-merge flag enabled for the
-- keyless `()` set, so it emitted one grand-total row per input block instead of a single merged row.

-- The `()` set is identified by `grouping(k) = 1`. It must be a single row whose count is the full input size,
-- independent of how the input was split into blocks.
SELECT count(), sum(c)
FROM
(
    SELECT grouping(k) AS g, count() AS c
    FROM (SELECT number % 1000 AS k FROM numbers(1000000))
    GROUP BY GROUPING SETS ((k), ())
    SETTINGS group_by_each_block_no_merge = 1, max_block_size = 10000
)
WHERE g = 1;

-- With several non-empty sets and the empty set, the non-empty sets keep the per-block flush (their clones have
-- keys), while the empty set must still be merged into a single row.
SELECT count(), sum(c)
FROM
(
    SELECT grouping(k1) AS g1, grouping(k2) AS g2, count() AS c
    FROM (SELECT number % 1000 AS k1, number % 7 AS k2 FROM numbers(1000000))
    GROUP BY GROUPING SETS ((k1), (k2), ())
    SETTINGS group_by_each_block_no_merge = 1, max_block_size = 10000
)
WHERE g1 = 1 AND g2 = 1;
