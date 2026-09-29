-- Regression test for a LOGICAL_ERROR ("Bad cast from ColumnString to ColumnLowCardinality") during
-- primary-key index analysis over a LowCardinality key wrapped in a nested CAST chain that
-- re-introduces LowCardinality mid-chain, e.g. CAST(CAST(s, 'LowCardinality(String)'), 'String').
-- Each chain function is built against the previous one's result type, so an inner
-- CAST(..., 'LowCardinality(String)') legitimately makes the next wrapper declare a LowCardinality
-- argument. applyFunction used to cache every intermediate result with LowCardinality stripped from
-- both the type and the column, so that next wrapper (which has
-- useDefaultImplementationForLowCardinalityColumns = false) received a full ColumnString and its
-- checkAndGetColumn<ColumnLowCardinality> aborted (in debug/sanitizer) or failed the query (in
-- release). The cache now keeps each function's own result type and representation, so every function
-- receives exactly the argument type it was built for.

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

-- Same regression through a typed LowCardinality ALIAS over a numeric key (plus a skip index),
-- distinct from the String CAST variants above.
DROP TABLE IF EXISTS t_04612_alias;
CREATE TABLE t_04612_alias
    (a UInt64, x LowCardinality(UInt64) ALIAS a + 1, y UInt64 ALIAS x * 2,
     INDEX idx y TYPE bloom_filter GRANULARITY 1)
    ENGINE = MergeTree ORDER BY a
    SETTINGS index_granularity = 8, allow_suspicious_low_cardinality_types = 1;
INSERT INTO t_04612_alias SELECT number FROM numbers(1000);

-- Previously aborted in KeyCondition; PK-pruned count checked against a brute-force scan.
SELECT count() = (SELECT countIf(y = 1048576) FROM t_04612_alias) FROM t_04612_alias WHERE y = 1048576;
SELECT count() = (SELECT countIf(y = 1000) FROM t_04612_alias) FROM t_04612_alias WHERE y = 1000;
SELECT count() FROM t_04612_alias WHERE y = 1000;

-- Pruning must still fire over the typed-ALIAS chain (not a silent index-disabling fallback).
SELECT
    toUInt32(extract(g, '^(\d+)')) < toUInt32(extract(g, '/(\d+)$')) AS pruning_fired
FROM (
    SELECT extract(trimLeft(explain), 'Granules: (\d+/\d+)') AS g
    FROM (
        EXPLAIN indexes = 1
        SELECT count() FROM t_04612_alias WHERE y = 1000
    )
    WHERE explain ILIKE '%Granules: %/%'
);

DROP TABLE t_04612_alias;

-- Same nested-cast chain over a LowCardinality PARTITION key. Unlike the WHERE cases above (which
-- run through applyFunction's cached-column branch), partition/minmax pruning feeds explicit Field
-- bounds through applyFunctionForField, so this covers the second half of the fix.
DROP TABLE IF EXISTS t_04612_part;
CREATE TABLE t_04612_part (s LowCardinality(Nullable(Int32)), v UInt32)
    ENGINE = MergeTree PARTITION BY s ORDER BY v
    SETTINGS allow_nullable_key = 1;
INSERT INTO t_04612_part SELECT number, number FROM numbers(20);
INSERT INTO t_04612_part SELECT number + 1000, number FROM numbers(20);

SELECT count() = (SELECT countIf(CAST(CAST(s, 'LowCardinality(String)'), 'String') < '5') FROM t_04612_part)
    FROM t_04612_part WHERE CAST(CAST(s, 'LowCardinality(String)'), 'String') < '5';

-- At least one index (partition/minmax) must prune parts (a "Parts: X/Y" line with X < Y).
SELECT max(toUInt32(extract(g, '^(\d+)')) < toUInt32(extract(g, '/(\d+)$'))) AS pruning_fired
FROM (
    SELECT extract(trimLeft(explain), 'Parts: (\d+/\d+)') AS g
    FROM (
        EXPLAIN indexes = 1
        SELECT count() FROM t_04612_part WHERE CAST(CAST(s, 'LowCardinality(String)'), 'String') < '5'
        SETTINGS optimize_use_implicit_projections = 0, optimize_trivial_count_query = 0
    )
    WHERE explain ILIKE '%Parts: %/%'
);

-- The set-index path (IN, and `has` over a constant array) applies the same chain through
-- MergeTreeSetIndex::checkInRange, which passes the key column's raw (still LowCardinality) type. Both
-- of these aborted with "Bad cast from ColumnLowCardinality to ColumnNullable" until
-- applyMonotonicFunctionsChainToRange normalized the incoming type itself.
SELECT count() = (SELECT countIf(CAST(CAST(s, 'LowCardinality(String)'), 'String') IN ('1', '2', '3')) FROM t_04612_part)
    FROM t_04612_part WHERE CAST(CAST(s, 'LowCardinality(String)'), 'String') IN ('1', '2', '3');
SELECT count() = (SELECT countIf(has(['1', '2', '3'], CAST(CAST(s, 'LowCardinality(String)'), 'String'))) FROM t_04612_part)
    FROM t_04612_part WHERE has(['1', '2', '3'], CAST(CAST(s, 'LowCardinality(String)'), 'String'));
SELECT count() FROM t_04612_part WHERE CAST(CAST(s, 'LowCardinality(String)'), 'String') IN ('1', '2', '3');

-- The set-index path must still prune parts, not merely return the right count: a chain whose types
-- disagree makes MergeTreeSetIndex decline silently and scan everything, which count equality alone
-- cannot detect.
SELECT max(toUInt32(extract(g, '^(\d+)')) < toUInt32(extract(g, '/(\d+)$'))) AS pruning_fired
FROM (
    SELECT extract(trimLeft(explain), 'Parts: (\d+/\d+)') AS g
    FROM (
        EXPLAIN indexes = 1
        SELECT count() FROM t_04612_part WHERE CAST(CAST(s, 'LowCardinality(String)'), 'String') IN ('1', '2', '3')
        SETTINGS optimize_use_implicit_projections = 0, optimize_trivial_count_query = 0
    )
    WHERE explain ILIKE '%Parts: %/%'
);
SELECT max(toUInt32(extract(g, '^(\d+)')) < toUInt32(extract(g, '/(\d+)$'))) AS pruning_fired
FROM (
    SELECT extract(trimLeft(explain), 'Parts: (\d+/\d+)') AS g
    FROM (
        EXPLAIN indexes = 1
        SELECT count() FROM t_04612_part WHERE has(['1', '2', '3'], CAST(CAST(s, 'LowCardinality(String)'), 'String'))
        SETTINGS optimize_use_implicit_projections = 0, optimize_trivial_count_query = 0
    )
    WHERE explain ILIKE '%Parts: %/%'
);

DROP TABLE t_04612_part;

-- A comparison whose constant needs a supertype cast appended after the chain. `extractAtomFromTree`
-- strips LowCardinality from the key type to pick that supertype, but the chain's last function still
-- returns LowCardinality, so the appended cast must declare the type it is actually given. Declaring
-- the stripped type instead made this fail with "Illegal column LowCardinality(Int32) of first
-- argument of function toDateTime64" (ILLEGAL_COLUMN). A LowCardinality PARTITION key routes this
-- through applyFunctionForField, the explicit-Field path.
DROP TABLE IF EXISTS t_04612_super;
CREATE TABLE t_04612_super (d LowCardinality(Date), v UInt32)
    ENGINE = MergeTree ORDER BY tuple() PARTITION BY d
    SETTINGS index_granularity = 8;
INSERT INTO t_04612_super SELECT toDate('2020-01-01') + number, number FROM numbers(4);

SELECT count() = (SELECT countIf(CAST(d, 'LowCardinality(Date32)') < toDateTime('2020-01-03 00:00:00')) FROM t_04612_super)
    FROM t_04612_super WHERE CAST(d, 'LowCardinality(Date32)') < toDateTime('2020-01-03 00:00:00');
SELECT count() FROM t_04612_super WHERE CAST(d, 'LowCardinality(Date32)') < toDateTime('2020-01-03 00:00:00');

-- Partition pruning must still fire for that predicate.
SELECT max(toUInt32(extract(g, '^(\d+)')) < toUInt32(extract(g, '/(\d+)$'))) AS pruning_fired
FROM (
    SELECT extract(trimLeft(explain), 'Parts: (\d+/\d+)') AS g
    FROM (
        EXPLAIN indexes = 1
        SELECT count() FROM t_04612_super WHERE CAST(d, 'LowCardinality(Date32)') < toDateTime('2020-01-03 00:00:00')
        SETTINGS optimize_use_implicit_projections = 0, optimize_trivial_count_query = 0
    )
    WHERE explain ILIKE '%Parts: %/%'
);

DROP TABLE t_04612_super;
