-- https://github.com/ClickHouse/ClickHouse/issues/117225
-- `dictGetHierarchy` returns an empty array for every key the dictionary does not have, so distinct
-- absent keys collide and the function is not injective. The default-on
-- `optimize_injective_functions_in_group_by` used to trust the claim, drop the function from
-- `GROUP BY` and group by the raw key instead, returning one `[]` row per absent key.

DROP TABLE IF EXISTS t_hier_src;
DROP DICTIONARY IF EXISTS d_hier;

CREATE TABLE t_hier_src (id UInt64, parent_id UInt64) ENGINE = Memory;
INSERT INTO t_hier_src VALUES (1, 0), (2, 1);
CREATE DICTIONARY d_hier (id UInt64, parent_id UInt64 HIERARCHICAL)
PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 't_hier_src')) LAYOUT(FLAT()) LIFETIME(0);

SELECT dictGetHierarchy('d_hier', k) AS h, count() FROM (SELECT arrayJoin([toUInt64(100), toUInt64(200)]) AS k) GROUP BY h;
SELECT dictGetHierarchy('d_hier', k) AS h, count() FROM (SELECT arrayJoin([toUInt64(100), toUInt64(200)]) AS k) GROUP BY h
SETTINGS optimize_injective_functions_in_group_by = 0;

SELECT 'the legacy analyzer has its own injective-function elimination in TreeOptimizer';
SELECT dictGetHierarchy('d_hier', k) AS h, count() FROM (SELECT arrayJoin([toUInt64(100), toUInt64(200)]) AS k) GROUP BY dictGetHierarchy('d_hier', k)
SETTINGS enable_analyzer = 0;
SELECT dictGetHierarchy('d_hier', k) AS h, count() FROM (SELECT arrayJoin([toUInt64(100), toUInt64(200)]) AS k) GROUP BY h
SETTINGS enable_analyzer = 0;
SELECT uniqExact(dictGetHierarchy('d_hier', k)) FROM (SELECT arrayJoin([toUInt64(100), toUInt64(200), toUInt64(300)]) AS k)
SETTINGS enable_analyzer = 0;

SELECT 'keys present in the dictionary are unaffected';
SELECT dictGetHierarchy('d_hier', k) AS h, count() FROM (SELECT arrayJoin([toUInt64(1), toUInt64(2)]) AS k) GROUP BY h ORDER BY h;

SELECT 'the function is no longer eliminated from GROUP BY';
-- `EXPLAIN QUERY TREE` exists only in the analyzer; the legacy path is pinned by the `enable_analyzer = 0` rows above.
SET enable_analyzer = 1;
SELECT count() > 0 FROM (EXPLAIN QUERY TREE SELECT dictGetHierarchy('d_hier', k) AS h, count() FROM (SELECT arrayJoin([toUInt64(100), toUInt64(200)]) AS k) GROUP BY h)
WHERE explain LIKE '%dictGetHierarchy%';

SELECT 'uniqExact over absent keys';
SELECT uniqExact(dictGetHierarchy('d_hier', k)) FROM (SELECT arrayJoin([toUInt64(100), toUInt64(200), toUInt64(300)]) AS k);

DROP DICTIONARY d_hier;
DROP TABLE t_hier_src;
