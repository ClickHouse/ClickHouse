-- Regression test for a LOGICAL_ERROR ("Bad cast from ColumnString to ColumnLowCardinality") during
-- primary-key index analysis over a LowCardinality key wrapped in a nested CAST chain that
-- re-introduces LowCardinality mid-chain, e.g. CAST(CAST(s, 'LowCardinality(String)'), 'String').
-- The monotonic-function chain is built against the recursively-stripped (non-LowCardinality) key
-- type, but an inner CAST wrapper is resolved for a LowCardinality source, so KeyCondition fed it a
-- full ColumnString and its checkAndGetColumn<ColumnLowCardinality> aborted (in debug/sanitizer) or
-- failed the query (in release). applyFunction / applyFunctionForField now match the argument
-- column's LowCardinality-ness to the function's declared argument type (re-wrap or strip), like the
-- sibling applyFunctionChainToColumn.

SET allow_suspicious_low_cardinality_types = 1;

DROP TABLE IF EXISTS t_04612;

CREATE TABLE t_04612 (s LowCardinality(Nullable(Int32)))
    ENGINE = MergeTree ORDER BY s
    SETTINGS index_granularity = 8, allow_nullable_key = 1;
INSERT INTO t_04612 SELECT number FROM numbers(20);
INSERT INTO t_04612 SELECT number + 1000 FROM numbers(20);

-- Each of these previously aborted in KeyCondition. The PK-pruned count (WHERE) is checked against a
-- brute-force scan (countIf over the same predicate) so pruning stays correct, not just non-crashing.
SELECT count() = (SELECT countIf(CAST(CAST(s, 'LowCardinality(String)'), 'String') < '5') FROM t_04612)
    FROM t_04612 WHERE CAST(CAST(s, 'LowCardinality(String)'), 'String') < '5';
SELECT count() = (SELECT countIf(CAST(CAST(s, 'LowCardinality(String)'), 'Nullable(String)') < '5') FROM t_04612)
    FROM t_04612 WHERE CAST(CAST(s, 'LowCardinality(String)'), 'Nullable(String)') < '5';
SELECT count() = (SELECT countIf(CAST(CAST(CAST(s, 'LowCardinality(String)'), 'String'), 'String') < '5') FROM t_04612)
    FROM t_04612 WHERE CAST(CAST(CAST(s, 'LowCardinality(String)'), 'String'), 'String') < '5';
SELECT count() = (SELECT countIf(CAST(CAST(s, 'LowCardinality(String)'), 'Nullable(FixedString(8))') < '5') FROM t_04612)
    FROM t_04612 WHERE CAST(CAST(s, 'LowCardinality(String)'), 'Nullable(FixedString(8))') < '5';

-- The concrete pruned count, for a stable reference.
SELECT count() FROM t_04612 WHERE CAST(CAST(s, 'LowCardinality(String)'), 'String') < '5';

-- Pruning must still fire (a "safe fallback" that silently disables the index would be a regression
-- that a correct-results check cannot catch): a selective nested-cast predicate over a numeric
-- LowCardinality key must read only a small fraction of granules. Assert read_granules < total_granules.
DROP TABLE IF EXISTS t_04612_prune;
CREATE TABLE t_04612_prune (s LowCardinality(Int64)) ENGINE = MergeTree ORDER BY s SETTINGS index_granularity = 8;
INSERT INTO t_04612_prune SELECT number FROM numbers(1000);
SELECT
    toUInt32(extract(g, '^(\d+)')) < toUInt32(extract(g, '/(\d+)$')) AS pruning_fired
FROM (
    SELECT extract(trimLeft(explain), 'Granules: (\d+/\d+)') AS g
    FROM (
        EXPLAIN indexes = 1
        SELECT count() FROM t_04612_prune WHERE toInt64(CAST(s, 'LowCardinality(Int64)')) BETWEEN 40 AND 45
    )
    WHERE explain ILIKE '%Granules: %/%'
);

DROP TABLE t_04612;
DROP TABLE t_04612_prune;
