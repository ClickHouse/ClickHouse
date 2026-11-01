-- Regression test for a `set` skip index that stops pruning granules for
-- function predicates (`hasToken`, `LIKE`, ...) over a non-nullable
-- `LowCardinality(String)` column.
--
-- Caused by https://github.com/ClickHouse/ClickHouse/pull/105384 . That PR
-- guards the `__bitWrapperFunc` wrapping in
-- `MergeTreeIndexConditionSet::traverseDAG` with
-- `WhichDataType(removeNullable(atom->result_type)).isInteger()`, falling back
-- to `UNKNOWN_FIELD` (no pruning) otherwise. The check strips `Nullable` but
-- not `LowCardinality`: for a `LowCardinality(String)` column, `hasToken` and
-- `LIKE` produce `LowCardinality(UInt8)`, which is not "integer", so every atom
-- becomes `UNKNOWN_FIELD` and the `set` index prunes nothing -- turning an
-- index lookup into a full scan. A plain `String` column returns `UInt8` and is
-- unaffected, which is why the regression is specific to `LowCardinality`.
--
-- The index lookup is correct (the wrapped `LowCardinality(UInt8)` atom matches
-- the granule's stored values), so results are unchanged -- only pruning is
-- lost. Hence this test asserts on the number of granules the index keeps, via
-- `EXPLAIN indexes = 1`, rather than on query results.

DROP TABLE IF EXISTS t_set_lc;

CREATE TABLE t_set_lc
(
    s LowCardinality(String),
    INDEX idx_set s TYPE set(100) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 1;

SET explain_query_plan_default = 'legacy';

-- 16 rows, one distinct value per granule (GRANULARITY 1, index_granularity 1),
-- so a predicate matching a single value must leave exactly one granule.
INSERT INTO t_set_lc SELECT 'svc' || leftPad(toString(number), 4, '0') FROM numbers(16);

-- Pin the settings that gate skip-index analysis so the stateless runner's
-- setting randomization cannot mask the regression.

-- `hasToken`: the set index must prune to a single granule (expect `1/16`).
SELECT trimLeft(explain)
FROM
(
    EXPLAIN indexes = 1
    SELECT count() FROM t_set_lc WHERE hasToken(s, 'svc0003')
    SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0, use_query_condition_cache = 0
)
WHERE explain LIKE '%Granules:%';

-- `LIKE`: same expectation.
SELECT trimLeft(explain)
FROM
(
    EXPLAIN indexes = 1
    SELECT count() FROM t_set_lc WHERE s LIKE '%svc0003%'
    SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0, use_query_condition_cache = 0
)
WHERE explain LIKE '%Granules:%';

-- Results stay correct regardless of pruning (sanity check).
SELECT count() FROM t_set_lc WHERE hasToken(s, 'svc0003');
SELECT count() FROM t_set_lc WHERE s LIKE '%svc0003%';

DROP TABLE t_set_lc;
