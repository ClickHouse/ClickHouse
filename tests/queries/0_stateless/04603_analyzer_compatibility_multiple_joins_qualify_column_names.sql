-- Compatibility setting `analyzer_compatibility_multiple_joins_qualify_column_names`
-- makes the analyzer mimic the old analyzer's multiple-joins column-naming
-- rewrite (when the `FROM` clause has two or more `JOIN`s), so that outer queries
-- referencing hidden inner aliases (e.g. `SELECT ll.Date FROM (SELECT * FROM t AS ll
-- LEFT JOIN x ... LEFT JOIN y ...)`) resolve again.
--
-- NOTE: this test encodes the FINAL expected behavior of the feature. The setting
-- itself is added by this change, but the analyzer hooks that implement it are added
-- by a later change. Every "setting ON" section below is EXPECTED TO FAIL until then.

SET enable_analyzer = 1;

-- ============================================================
-- Family 2 reproducers (the point of the feature)
-- ============================================================

SET analyzer_compatibility_multiple_joins_qualify_column_names = 0;

SELECT '=== family2: repro1 ll.Date, setting OFF (throws) ===';
SELECT ll.Date FROM (SELECT * FROM (SELECT 1 AS k, 'D' AS Date) AS ll LEFT JOIN (SELECT 1 AS k) AS t1 ON ll.k = t1.k LEFT JOIN (SELECT 1 AS k) AS t2 ON ll.k = t2.k); -- { serverError UNKNOWN_IDENTIFIER }

SELECT '=== family2: repro2 a.x, setting OFF (throws) ===';
SELECT a.x FROM (SELECT a.x FROM (SELECT 1 AS k, 'X' AS x) AS a LEFT JOIN (SELECT 1 AS k) AS b ON a.k = b.k LEFT JOIN (SELECT 1 AS k) AS c ON a.k = c.k); -- { serverError UNKNOWN_IDENTIFIER }

SET analyzer_compatibility_multiple_joins_qualify_column_names = 1;

SELECT '=== family2: repro1 ll.Date, setting ON ===';
SELECT ll.Date FROM (SELECT * FROM (SELECT 1 AS k, 'D' AS Date) AS ll LEFT JOIN (SELECT 1 AS k) AS t1 ON ll.k = t1.k LEFT JOIN (SELECT 1 AS k) AS t2 ON ll.k = t2.k);

SELECT '=== family2: repro2 a.x, setting ON ===';
SELECT a.x FROM (SELECT a.x FROM (SELECT 1 AS k, 'X' AS x) AS a LEFT JOIN (SELECT 1 AS k) AS b ON a.k = b.k LEFT JOIN (SELECT 1 AS k) AS c ON a.k = c.k);

SELECT '=== family2: repro1 GROUP BY ALL, setting ON ===';
SELECT ll.Date AS Date, count() FROM (SELECT * FROM (SELECT 1 AS k, 'D' AS Date) AS ll LEFT JOIN (SELECT 1 AS k) AS t1 ON ll.k = t1.k LEFT JOIN (SELECT 1 AS k) AS t2 ON ll.k = t2.k) GROUP BY ALL;

-- ============================================================
-- DESCRIBE parity matrix (setting ON) -- compare against old analyzer's names
-- ============================================================

SELECT '=== describe: 2-join star, setting ON ===';
DESCRIBE (SELECT * FROM (SELECT 1 AS k, 'D' AS Date) AS ll LEFT JOIN (SELECT 1 AS k) AS t1 ON ll.k = t1.k LEFT JOIN (SELECT 1 AS k) AS t2 ON ll.k = t2.k);

SELECT '=== describe: qualified matcher ll.*, setting ON ===';
DESCRIBE (SELECT ll.* FROM (SELECT 1 AS k, 'D' AS Date) AS ll LEFT JOIN (SELECT 1 AS k) AS t1 ON ll.k = t1.k LEFT JOIN (SELECT 1 AS k) AS t2 ON ll.k = t2.k);

SELECT '=== describe: comma-join trigger, setting ON ===';
DESCRIBE (SELECT * FROM (SELECT 1 AS k, 'D' AS Date) AS ll, (SELECT 2 AS k) AS t1, (SELECT 3 AS k) AS t2);

SELECT '=== describe: explicit unique qualified identifier a.x, setting ON ===';
DESCRIBE (SELECT a.x FROM (SELECT 1 AS k, 'X' AS x) AS a LEFT JOIN (SELECT 1 AS k) AS b ON a.k = b.k LEFT JOIN (SELECT 1 AS k) AS c ON a.k = c.k);

SELECT '=== describe: explicit bare identifier x stays bare, setting ON ===';
DESCRIBE (SELECT x FROM (SELECT 1 AS k, 'X' AS x) AS a LEFT JOIN (SELECT 1 AS k) AS b ON a.k = b.k LEFT JOIN (SELECT 1 AS k) AS c ON a.k = c.k);

SELECT '=== describe: alias always wins a.x AS y, setting ON ===';
DESCRIBE (SELECT a.x AS y FROM (SELECT 1 AS k, 'X' AS x) AS a LEFT JOIN (SELECT 1 AS k) AS b ON a.k = b.k LEFT JOIN (SELECT 1 AS k) AS c ON a.k = c.k);

DROP TABLE IF EXISTS ta;
DROP TABLE IF EXISTS tb;
DROP TABLE IF EXISTS tc;
CREATE TABLE ta (x UInt8, ka UInt8) ENGINE = Memory;
CREATE TABLE tb (kb UInt8) ENGINE = Memory;
CREATE TABLE tc (kc UInt8) ENGINE = Memory;

SELECT '=== describe: real tables without aliases, setting ON ===';
DESCRIBE (SELECT * FROM ta JOIN tb ON ta.ka = tb.kb JOIN tc ON ta.ka = tc.kc);

SELECT '=== describe: real tables, explicit qualified column ta.x, setting ON ===';
DESCRIBE (SELECT ta.x FROM ta JOIN tb ON ta.ka = tb.kb JOIN tc ON ta.ka = tc.kc);

SELECT '=== describe: 1 join only -- unchanged even with setting ON ===';
DESCRIBE (SELECT * FROM (SELECT 1 AS k, 'D' AS Date) AS ll LEFT JOIN (SELECT 1 AS k) AS t1 ON ll.k = t1.k);

-- ============================================================
-- Documented deviations from the old analyzer (setting ON -- pin the NEW behavior)
-- ============================================================

SELECT '=== describe: function projection name unchanged (ll.k + 1), setting ON ===';
DESCRIBE (SELECT ll.k + 1 FROM (SELECT 1 AS k, 'D' AS Date) AS ll LEFT JOIN (SELECT 1 AS k) AS t1 ON ll.k = t1.k LEFT JOIN (SELECT 1 AS k) AS t2 ON ll.k = t2.k);

SELECT '=== describe: SELECT * over JOIN ... USING keeps merge semantics, setting ON ===';
DESCRIBE (SELECT * FROM (SELECT 1 AS k, 'D' AS Date) AS ll LEFT JOIN (SELECT 1 AS k) AS t1 USING (k) LEFT JOIN (SELECT 1 AS k) AS t2 ON ll.k = t2.k);

SET joined_subquery_requires_alias = 0;

SELECT '=== describe: unaliased joined subquery gets no prefix (old parity), setting ON ===';
DESCRIBE (SELECT * FROM (SELECT 1 AS k, 'D' AS Date) LEFT JOIN (SELECT 1 AS k2) AS t1 ON k = t1.k2 LEFT JOIN (SELECT 1 AS k3) AS t2 ON k = t2.k3);

SET joined_subquery_requires_alias = 1;

-- ============================================================
-- Setting OFF (default) -- byte-identical to today's default analyzer behavior
-- ============================================================

SET analyzer_compatibility_multiple_joins_qualify_column_names = 0;

SELECT '=== describe: 2-join star, setting OFF (today unchanged) ===';
DESCRIBE (SELECT * FROM (SELECT 1 AS k, 'D' AS Date) AS ll LEFT JOIN (SELECT 1 AS k) AS t1 ON ll.k = t1.k LEFT JOIN (SELECT 1 AS k) AS t2 ON ll.k = t2.k);

SELECT '=== describe: comma-join trigger, setting OFF (today unchanged) ===';
DESCRIBE (SELECT * FROM (SELECT 1 AS k, 'D' AS Date) AS ll, (SELECT 2 AS k) AS t1, (SELECT 3 AS k) AS t2);

SELECT '=== describe: real tables without aliases, setting OFF (today unchanged) ===';
DESCRIBE (SELECT * FROM ta JOIN tb ON ta.ka = tb.kb JOIN tc ON ta.ka = tc.kc);

DROP TABLE ta;
DROP TABLE tb;
DROP TABLE tc;

-- ============================================================
-- Nested / outer-scope usage
-- ============================================================

SET analyzer_compatibility_multiple_joins_qualify_column_names = 1;

SELECT '=== nested outer scope, setting ON ===';
SELECT ll.Date FROM (SELECT * FROM (SELECT * FROM (SELECT 1 AS k, 'D' AS Date) AS ll LEFT JOIN (SELECT 1 AS k) AS t1 ON ll.k = t1.k LEFT JOIN (SELECT 1 AS k) AS t2 ON ll.k = t2.k));

-- ============================================================
-- ARRAY JOIN mixed with 2 joins (setting ON). The old analyzer throws
-- (`Multiple JOIN does not support mix with ARRAY JOINs`); the analyzer already
-- supports this combination today. The exact qualified name for the ARRAY JOIN result
-- column `arr` is uncertain until the analyzer hooks land -- see report if it differs.
-- ============================================================

SELECT '=== describe: ARRAY JOIN mixed with 2 joins, setting ON ===';
DESCRIBE (SELECT * FROM (SELECT 1 AS k, [1, 2] AS arr, 'D' AS Date) AS ll LEFT JOIN (SELECT 1 AS k) AS t1 ON ll.k = t1.k LEFT JOIN (SELECT 1 AS k) AS t2 ON ll.k = t2.k ARRAY JOIN arr);

-- ============================================================
-- Distributed: the hidden-alias outer reference must survive going through `remote`
-- (aliases are attached in `toAST`, so the qualified projection name is preserved
-- across the shard boundary).
-- ============================================================

SELECT '=== distributed: remote() two-shard, family2 shape, setting ON ===';
SELECT ll.Date FROM remote('127.0.0.{1,2}', view(SELECT * FROM (SELECT 1 AS k, 'D' AS Date) AS ll LEFT JOIN (SELECT 1 AS k) AS t1 ON ll.k = t1.k LEFT JOIN (SELECT 1 AS k) AS t2 ON ll.k = t2.k)) ORDER BY ll.Date SETTINGS analyzer_compatibility_multiple_joins_qualify_column_names = 1;
