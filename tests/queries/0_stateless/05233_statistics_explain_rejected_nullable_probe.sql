-- The nullable-only `KeyCondition` probe must not leak rejected columns into `EXPLAIN`.
-- A range predicate on `value_for_range` can prune parts; `!=` / OR-nested `IS NULL` on
-- nullable-only `basic` columns cannot, so they must not appear under `Statistics` `Keys`.

SET allow_statistics = 1;
SET use_statistics_for_part_pruning = 1;
SET enable_analyzer = 1;
SET materialize_statistics_on_insert = 1;
SET allow_suspicious_low_cardinality_types = 1;

DROP TABLE IF EXISTS test_explain_nullable_probe;

CREATE TABLE test_explain_nullable_probe
(
    bucket UInt8,
    value_for_range Nullable(Int64) STATISTICS(basic),
    value_a Nullable(String) STATISTICS(basic),
    value_b Nullable(String) STATISTICS(basic),
    value_lc LowCardinality(Nullable(String)) STATISTICS(basic)
)
ENGINE = MergeTree()
PARTITION BY bucket
ORDER BY tuple()
-- `nullable_serialization_version` is pinned: randomized `allow_sparse` enables the
-- sparsity-filter trivial count rewrite for `count()`, which replaces ReadFromMergeTree
-- in EXPLAIN and hides the `Statistics` index section.
SETTINGS auto_statistics_types = '', nullable_serialization_version = 'basic';

-- Part 0: all NULL, pruned by `value_for_range > 150`.
INSERT INTO test_explain_nullable_probe VALUES (0, NULL, NULL, NULL, NULL);
-- Part 1: below-range, pruned by `value_for_range > 150`.
INSERT INTO test_explain_nullable_probe VALUES (1, 100, 'a', 'a', 'a');
-- Part 2: inside the range, kept.
INSERT INTO test_explain_nullable_probe VALUES (2, 200, 'c', 'c', 'c');

SELECT 'Rejected `!=` probe is not listed as a `Statistics` key';
SELECT
    countIf(explain LIKE '%Statistics%') > 0,
    countIf(explain LIKE '%Parts: 1/3%') > 0,
    countIf(trim(explain) = 'value_for_range') > 0,
    countIf(trim(explain) = 'value_lc') = 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM test_explain_nullable_probe WHERE value_for_range > 150 AND value_lc != 'x');
SELECT count() FROM test_explain_nullable_probe WHERE value_for_range > 150 AND value_lc != 'x';

SELECT 'Rejected OR-nested `IS NULL` probe is not listed as a `Statistics` key';
SELECT
    countIf(explain LIKE '%Statistics%') > 0,
    countIf(explain LIKE '%Parts: 1/3%') > 0,
    countIf(trim(explain) = 'value_for_range') > 0,
    countIf(trim(explain) = 'value_a') = 0,
    countIf(trim(explain) = 'value_b') = 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM test_explain_nullable_probe WHERE value_for_range > 150 AND (value_a IS NULL OR value_b IS NOT NULL));
SELECT count() FROM test_explain_nullable_probe WHERE value_for_range > 150 AND (value_a IS NULL OR value_b IS NOT NULL);

SELECT 'Accepted nullable-only equality is listed as a `Statistics` key';
SELECT
    countIf(explain LIKE '%Statistics%') > 0,
    countIf(explain LIKE '%Parts: 1/3%') > 0,
    countIf(trim(explain) = 'value_for_range') > 0,
    countIf(trim(explain) = 'value_lc') > 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM test_explain_nullable_probe WHERE value_for_range > 150 AND value_lc = 'c');
SELECT count() FROM test_explain_nullable_probe WHERE value_for_range > 150 AND value_lc = 'c';

DROP TABLE test_explain_nullable_probe;
