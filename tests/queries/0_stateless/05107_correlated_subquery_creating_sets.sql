-- A set built inside a correlated subquery adds a `DelayedCreatingSets` step to the subquery's plan, which
-- the decorrelation refused ("Cannot decorrelate query, because 'DelayedCreatingSets' step is not
-- supported"). `optimize_inverse_dictionary_lookup` (on by default) builds such a set out of a
-- `dictGet(...) >= const` predicate, so an ordinary query failed at default settings.

-- Correlated subqueries are a feature of the analyzer, so this test needs it.
SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_dict_source;
DROP DICTIONARY IF EXISTS d_correlated_sets;
DROP TABLE IF EXISTS t_correlated_sets;

CREATE TABLE t_dict_source (id UInt64, val UInt32) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_dict_source SELECT number, number % 97 FROM numbers(500);

CREATE DICTIONARY d_correlated_sets (id UInt64, val UInt32 DEFAULT 0)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 't_dict_source' DB currentDatabase()))
LIFETIME(0)
LAYOUT(FLAT());

CREATE TABLE t_correlated_sets (k UInt32, g Int32, s String) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_correlated_sets SELECT number, number % 7, toString(number % 5) FROM numbers(100);

SELECT 'a dictGet comparison inside a correlated EXISTS';
SELECT count() FROM t_correlated_sets AS o WHERE EXISTS (
    SELECT 1 FROM t_correlated_sets AS i WHERE i.s = o.s AND dictGet(currentDatabase() || '.d_correlated_sets', 'val', toUInt64(abs(g)) % 500) >= 5);
SELECT count() FROM t_correlated_sets AS o WHERE EXISTS (
    SELECT 1 FROM t_correlated_sets AS i WHERE i.s = o.s AND dictGet(currentDatabase() || '.d_correlated_sets', 'val', toUInt64(abs(g)) % 500) >= 5)
SETTINGS optimize_inverse_dictionary_lookup = 0;

SELECT 'one that no row satisfies';
SELECT count() FROM t_correlated_sets AS o WHERE EXISTS (
    SELECT 1 FROM t_correlated_sets AS i WHERE i.s = o.s AND dictGet(currentDatabase() || '.d_correlated_sets', 'val', toUInt64(abs(g)) % 500) >= 76);

SELECT 'a correlated NOT EXISTS';
SELECT count() FROM t_correlated_sets AS o WHERE NOT EXISTS (
    SELECT 1 FROM t_correlated_sets AS i WHERE i.s = o.s AND dictGet(currentDatabase() || '.d_correlated_sets', 'val', toUInt64(abs(g)) % 500) >= 5);

SELECT 'the same set inside a correlated scalar subquery';
SELECT count() FROM t_correlated_sets AS o WHERE o.g >= (
    SELECT count() FROM t_correlated_sets AS i WHERE i.s = o.s AND dictGet(currentDatabase() || '.d_correlated_sets', 'val', toUInt64(abs(i.g)) % 500) >= 5);
SELECT o.k, (SELECT count() FROM t_correlated_sets AS i WHERE i.s = o.s AND dictGet(currentDatabase() || '.d_correlated_sets', 'val', toUInt64(abs(i.g)) % 500) >= 5)
FROM t_correlated_sets AS o ORDER BY o.k LIMIT 3;

SELECT 'an IN subquery inside a correlated EXISTS builds a set the same way';
SELECT count() FROM t_correlated_sets AS o WHERE EXISTS (
    SELECT 1 FROM t_correlated_sets AS i WHERE i.s = o.s AND i.k IN (SELECT k FROM t_correlated_sets WHERE k < 50));

-- A set source that is itself correlated must not slip through: the analyzer rejects a correlated `IN`
-- argument before planning, and `decorrelateQueryPlan` rejects a correlated set source as well, so such a
-- query fails with `NOT_IMPLEMENTED` instead of a `PLACEHOLDER` logical error from executing the set plan
-- standalone. Correlated on the subquery's own table:
SELECT count() FROM t_correlated_sets AS o WHERE EXISTS (
    SELECT 1 FROM t_correlated_sets AS i WHERE i.s = o.s AND i.k IN (SELECT n.k FROM t_correlated_sets AS n WHERE n.g = i.g)); -- { serverError NOT_IMPLEMENTED }
-- and on the outer table:
SELECT count() FROM t_correlated_sets AS o WHERE EXISTS (
    SELECT 1 FROM t_correlated_sets AS i WHERE i.s = o.s AND i.k IN (SELECT n.k FROM t_correlated_sets AS n WHERE n.g = o.g)); -- { serverError NOT_IMPLEMENTED }

DROP TABLE t_correlated_sets;
DROP DICTIONARY d_correlated_sets;
DROP TABLE t_dict_source;
