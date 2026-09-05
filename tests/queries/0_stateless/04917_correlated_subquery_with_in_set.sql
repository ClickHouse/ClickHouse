DROP TABLE IF EXISTS t_src;
DROP TABLE IF EXISTS t_main;
DROP DICTIONARY IF EXISTS d_src;

SET enable_analyzer = 1;

CREATE TABLE t_src (id UInt64, val UInt32) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_src SELECT number, number % 97 FROM numbers(500);

CREATE DICTIONARY d_src (id UInt64, val UInt32 DEFAULT 0)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 't_src' DB currentDatabase()))
LIFETIME(0)
LAYOUT(FLAT());

CREATE TABLE t_main (
    k UInt32,
    g Int32,
    g_null Nullable(Int32),
    g_lc LowCardinality(String),
    g_lc_null LowCardinality(Nullable(String)),
    s String
) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_main SELECT
    number,
    number % 7,
    if(number % 13 = 0, NULL, number % 7),
    toString(number % 5),
    if(number % 11 = 0, NULL, toString(number % 5)),
    toString(number % 5)
FROM numbers(100);

SELECT '-- dictGet comparison rewritten to an IN set inside a correlated EXISTS';
SELECT count() FROM t_main AS o WHERE EXISTS (
    SELECT 1 FROM t_main AS i
    WHERE i.s = o.s AND dictGet(currentDatabase() || '.d_src', 'val', toUInt64(i.k)) >= 3)
SETTINGS optimize_inverse_dictionary_lookup = 1;
SELECT count() FROM t_main AS o WHERE EXISTS (
    SELECT 1 FROM t_main AS i
    WHERE i.s = o.s AND dictGet(currentDatabase() || '.d_src', 'val', toUInt64(i.k)) >= 3)
SETTINGS optimize_inverse_dictionary_lookup = 0;
-- The rows above are equal either way, so they cannot witness the rewrite. These two assert that the
-- rewrite is what plants the set: a set step exists only when the rewrite is enabled.
SELECT count() > 0 FROM (
    EXPLAIN SELECT count() FROM t_main AS o WHERE EXISTS (
        SELECT 1 FROM t_main AS i
        WHERE i.s = o.s AND dictGet(currentDatabase() || '.d_src', 'val', toUInt64(i.k)) >= 3)
    SETTINGS optimize_inverse_dictionary_lookup = 1
) WHERE explain ILIKE '%CreatingSets%';
SELECT count() FROM (
    EXPLAIN SELECT count() FROM t_main AS o WHERE EXISTS (
        SELECT 1 FROM t_main AS i
        WHERE i.s = o.s AND dictGet(currentDatabase() || '.d_src', 'val', toUInt64(i.k)) >= 3)
    SETTINGS optimize_inverse_dictionary_lookup = 0
) WHERE explain ILIKE '%CreatingSets%';

SELECT '-- plain IN (SELECT ...) inside a correlated EXISTS';
SELECT count() FROM t_main AS o WHERE EXISTS (
    SELECT 1 FROM t_main AS i WHERE i.s = o.s AND i.g IN (SELECT val FROM t_src WHERE val <= 6));
SELECT count() FROM t_main AS o WHERE EXISTS (
    SELECT 1 FROM t_main AS i WHERE i.s = o.s AND i.g IN (0, 1, 2, 3, 4, 5, 6));
SELECT count() FROM t_main AS o WHERE EXISTS (
    SELECT 1 FROM t_main AS i WHERE i.s = o.s AND i.k < 8 AND i.g IN (SELECT val FROM t_src WHERE val = 3));
SELECT count() FROM t_main AS o WHERE EXISTS (
    SELECT 1 FROM t_main AS i WHERE i.s = o.s AND i.k < 8 AND i.g IN (3));

SELECT '-- partially matching IN set';
SELECT count() FROM t_main AS o WHERE EXISTS (
    SELECT 1 FROM t_main AS i WHERE i.s = o.s AND i.k < 20 AND i.g IN (SELECT val FROM t_src WHERE val = 3));
SELECT count() FROM t_main AS o WHERE EXISTS (
    SELECT 1 FROM t_main AS i WHERE i.s = o.s AND i.k < 20 AND i.g IN (3));

SELECT '-- IN set with no matching value';
SELECT count() FROM t_main AS o WHERE EXISTS (
    SELECT 1 FROM t_main AS i WHERE i.s = o.s AND i.g IN (SELECT val FROM t_src WHERE val > 90));
SELECT count() FROM t_main AS o WHERE EXISTS (
    SELECT 1 FROM t_main AS i WHERE i.s = o.s AND i.g IN (91, 92, 93, 94, 95, 96));

SELECT '-- empty IN set';
SELECT count() FROM t_main AS o WHERE EXISTS (
    SELECT 1 FROM t_main AS i WHERE i.s = o.s AND i.g IN (SELECT val FROM t_src WHERE val > 200));

SELECT '-- per-row equality against the constant-set rewrite';
-- Each arm selects a strict subset of the outer rows, so a per-row divergence would surface as a
-- non-zero symmetric difference. `join_use_nulls` is required: without it the unmatched side is
-- filled with defaults and `IS NULL` never holds.
SELECT count() FROM (
    SELECT o.k FROM t_main AS o WHERE EXISTS (
        SELECT 1 FROM t_main AS i WHERE i.s = o.s AND i.k < 20 AND i.g IN (SELECT val FROM t_src WHERE val = 3))
) AS a
FULL JOIN (
    SELECT o.k FROM t_main AS o WHERE EXISTS (
        SELECT 1 FROM t_main AS i WHERE i.s = o.s AND i.k < 20 AND i.g IN (3))
) AS b ON a.k = b.k
WHERE a.k IS NULL OR b.k IS NULL
SETTINGS join_use_nulls = 1;

SELECT '-- Nullable key';
SELECT count() FROM t_main AS o WHERE EXISTS (
    SELECT 1 FROM t_main AS i WHERE i.s = o.s AND i.k < 20 AND i.g_null IN (SELECT val FROM t_src WHERE val = 3));
SELECT count() FROM t_main AS o WHERE EXISTS (
    SELECT 1 FROM t_main AS i WHERE i.s = o.s AND i.k < 20 AND i.g_null IN (3));

SELECT '-- LowCardinality key';
SELECT count() FROM t_main AS o WHERE EXISTS (
    SELECT 1 FROM t_main AS i WHERE i.s = o.s AND i.k < 20 AND i.g_lc IN (SELECT toString(val) FROM t_src WHERE val = 3));
SELECT count() FROM t_main AS o WHERE EXISTS (
    SELECT 1 FROM t_main AS i WHERE i.s = o.s AND i.k < 20 AND i.g_lc IN ('3'));

SELECT '-- LowCardinality(Nullable) key';
SELECT count() FROM t_main AS o WHERE EXISTS (
    SELECT 1 FROM t_main AS i WHERE i.s = o.s AND i.k < 20 AND i.g_lc_null IN (SELECT toString(val) FROM t_src WHERE val = 3));
SELECT count() FROM t_main AS o WHERE EXISTS (
    SELECT 1 FROM t_main AS i WHERE i.s = o.s AND i.k < 20 AND i.g_lc_null IN ('3'));

SELECT '-- tuple key';
SELECT count() FROM t_main AS o WHERE EXISTS (
    SELECT 1 FROM t_main AS i WHERE i.s = o.s AND (i.g, i.k) IN (SELECT val, id FROM t_src WHERE id < 3));
SELECT count() FROM t_main AS o WHERE EXISTS (
    SELECT 1 FROM t_main AS i WHERE i.s = o.s AND (i.g, i.k) IN (
        SELECT number, number FROM numbers(3)));

SELECT '-- constant on the left of IN';
SELECT count() FROM t_main AS o WHERE EXISTS (
    SELECT 1 FROM t_main AS i WHERE i.s = o.s AND 999 IN (SELECT val FROM t_src WHERE val <= 6));
SELECT count() FROM t_main AS o WHERE EXISTS (
    SELECT 1 FROM t_main AS i WHERE i.s = o.s AND 999 IN (0, 1, 2, 3, 4, 5, 6));

SELECT '-- correlated scalar subquery';
SELECT count() FROM t_main AS o WHERE o.g >= (
    SELECT count() FROM t_main AS i WHERE i.s = o.s AND i.k < 20 AND i.g IN (SELECT val FROM t_src WHERE val = 3));
SELECT count() FROM t_main AS o WHERE o.g >= (
    SELECT count() FROM t_main AS i WHERE i.s = o.s AND i.k < 20 AND i.g IN (3));

SELECT '-- NOT IN';
SELECT count() FROM t_main AS o WHERE EXISTS (
    SELECT 1 FROM t_main AS i WHERE i.s = o.s AND i.k < 12 AND i.g NOT IN (SELECT val FROM t_src WHERE val <= 2));
SELECT count() FROM t_main AS o WHERE EXISTS (
    SELECT 1 FROM t_main AS i WHERE i.s = o.s AND i.k < 12 AND i.g NOT IN (0, 1, 2));

SELECT '-- GLOBAL IN';
SELECT count() FROM t_main AS o WHERE EXISTS (
    SELECT 1 FROM t_main AS i WHERE i.s = o.s AND i.k < 20 AND i.g GLOBAL IN (SELECT val FROM t_src WHERE val = 3));
SELECT count() FROM t_main AS o WHERE EXISTS (
    SELECT 1 FROM t_main AS i WHERE i.s = o.s AND i.k < 20 AND i.g IN (3));

SELECT '-- correlated NOT EXISTS';
SELECT count() FROM t_main AS o WHERE NOT EXISTS (
    SELECT 1 FROM t_main AS i WHERE i.s = o.s AND i.k < 20 AND i.g IN (SELECT val FROM t_src WHERE val = 3));
SELECT count() FROM t_main AS o WHERE NOT EXISTS (
    SELECT 1 FROM t_main AS i WHERE i.s = o.s AND i.k < 20 AND i.g IN (3));

SELECT '-- IN sets in both arms of a UNION ALL body';
SELECT count() FROM t_main AS o WHERE EXISTS (
    SELECT 1 FROM t_main AS i WHERE i.s = o.s AND i.k < 20 AND i.g IN (SELECT val FROM t_src WHERE val = 3)
    UNION ALL
    SELECT 1 FROM t_main AS j WHERE j.s = o.s AND j.k < 20 AND j.g IN (SELECT val FROM t_src WHERE val = 5));
SELECT count() FROM t_main AS o WHERE EXISTS (
    SELECT 1 FROM t_main AS i WHERE i.s = o.s AND i.k < 20 AND i.g IN (3)
    UNION ALL
    SELECT 1 FROM t_main AS j WHERE j.s = o.s AND j.k < 20 AND j.g IN (5));

SELECT '-- IN set beneath an aggregation';
SELECT count() FROM t_main AS o WHERE o.g >= (
    SELECT count() FROM (
        SELECT i.g FROM t_main AS i WHERE i.s = o.s AND i.k < 20 AND i.g IN (SELECT val FROM t_src WHERE val = 3)
    ) GROUP BY g);
SELECT count() FROM t_main AS o WHERE o.g >= (
    SELECT count() FROM (
        SELECT i.g FROM t_main AS i WHERE i.s = o.s AND i.k < 20 AND i.g IN (3)
    ) GROUP BY g);

SELECT '-- two independent IN sets in one body';
-- Both sets must narrow the result: dropping either one changes the count (80 without the g-set,
-- 40 without the k-set), so neither conjunct is implied by `i.k < 8`.
SELECT count() FROM t_main AS o WHERE EXISTS (
    SELECT 1 FROM t_main AS i WHERE i.s = o.s AND i.k < 8
      AND i.g IN (SELECT val FROM t_src WHERE val IN (3, 4))
      AND i.k IN (SELECT id FROM t_src WHERE id < 4));
SELECT count() FROM t_main AS o WHERE EXISTS (
    SELECT 1 FROM t_main AS i WHERE i.s = o.s AND i.k < 8 AND i.g IN (3, 4) AND i.k < 4);

SELECT '-- two-level nested correlated subquery, IN set in the inner body';
SELECT count() FROM t_main AS o WHERE EXISTS (
    SELECT 1 FROM t_main AS m WHERE m.s = o.s AND EXISTS (
        SELECT 1 FROM t_main AS n WHERE n.k = m.k AND n.k < 20 AND n.g IN (SELECT val FROM t_src WHERE val = 3)));
SELECT count() FROM t_main AS o WHERE EXISTS (
    SELECT 1 FROM t_main AS m WHERE m.s = o.s AND EXISTS (
        SELECT 1 FROM t_main AS n WHERE n.k = m.k AND n.k < 20 AND n.g IN (3)));

SELECT '-- a correlated set subquery is still rejected';
SELECT count() FROM t_main AS o WHERE EXISTS (
    SELECT 1 FROM t_main AS i WHERE i.s = o.s AND i.g IN (SELECT g FROM t_main WHERE s = o.s)); -- { serverError NOT_IMPLEMENTED }

SELECT '-- an ordinary CTE body without an IN set is unaffected';
WITH c AS (SELECT k, g, s FROM t_main)
SELECT count() FROM t_main AS o WHERE EXISTS (SELECT 1 FROM c AS i WHERE i.s = o.s);

SELECT '-- unchanged when parallel replicas are requested';
-- Parallel replicas are declined for this shape whatever is requested: the query-tree path disables
-- them for any correlated query, and the plan-based path runs a plan carrying an unmaterialized set
-- locally. So this asserts the request is harmless, not that replicas were used.
SELECT count() FROM t_main AS o WHERE EXISTS (
    SELECT 1 FROM t_main AS i WHERE i.s = o.s AND i.k < 20 AND i.g IN (SELECT val FROM t_src WHERE val = 3))
SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 3;

SELECT '-- the delayed step is expanded before execution and does not survive';
SELECT count() > 0 FROM (
    EXPLAIN SELECT count() FROM t_main AS o WHERE EXISTS (
        SELECT 1 FROM t_main AS i WHERE i.s = o.s AND i.g IN (SELECT val FROM t_src WHERE val <= 6))
) WHERE explain ILIKE '%CreatingSets%';
SELECT count() FROM (
    EXPLAIN SELECT count() FROM t_main AS o WHERE EXISTS (
        SELECT 1 FROM t_main AS i WHERE i.s = o.s AND i.g IN (SELECT val FROM t_src WHERE val <= 6))
) WHERE explain ILIKE '%DelayedCreatingSets%';

SELECT '-- the same query without a set subquery builds no set at all';
SELECT count() > 0 FROM (
    EXPLAIN SELECT count() FROM t_main AS o WHERE EXISTS (
        SELECT 1 FROM t_main AS i WHERE i.s = o.s AND i.g IN (0, 1, 2, 3, 4, 5, 6))
) WHERE explain ILIKE '%CreatingSets%';
SELECT count() FROM (
    EXPLAIN SELECT count() FROM t_main AS o WHERE EXISTS (
        SELECT 1 FROM t_main AS i WHERE i.s = o.s AND i.g IN (0, 1, 2, 3, 4, 5, 6))
) WHERE explain ILIKE '%DelayedCreatingSets%';

DROP DICTIONARY d_src;
DROP TABLE t_main;
DROP TABLE t_src;
