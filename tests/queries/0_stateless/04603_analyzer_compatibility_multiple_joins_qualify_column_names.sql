-- Compatibility setting `analyzer_compatibility_multiple_joins_qualify_column_names`
-- makes the analyzer mimic the old analyzer's multiple-joins column-naming
-- rewrite (when the `FROM` clause has two or more `JOIN`s), so that outer queries
-- referencing hidden inner aliases (e.g. `SELECT ll.Date FROM (SELECT * FROM t AS ll
-- LEFT JOIN x ... LEFT JOIN y ...)`) resolve again.

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

-- ============================================================
-- `COLUMNS` matcher: the identifier-list form `COLUMNS(col1, col2)` is not a
-- matcher expansion (it is resolved as a plain list of column references), so
-- with the setting on each column keeps the name exactly as its identifier was
-- written. The setting never adds a qualifier here, unlike the regexp form
-- `COLUMNS('<regexp>')`, which goes through the same expansion as `*`. An item
-- written without a qualifier therefore stays bare, on the old analyzer as well.
-- ============================================================

SET analyzer_compatibility_multiple_joins_qualify_column_names = 1;

SELECT '=== describe: COLUMNS(col) identifier-list form stays bare, setting ON ===';
DESCRIBE (SELECT COLUMNS(Date) FROM (SELECT 1 AS k, 'D' AS Date) AS ll LEFT JOIN (SELECT 1 AS k) AS t1 ON ll.k = t1.k LEFT JOIN (SELECT 1 AS k) AS t2 ON ll.k = t2.k);

SELECT '=== describe: COLUMNS(regexp) form is qualified like * (old parity), setting ON ===';
DESCRIBE (SELECT COLUMNS('^Date$') FROM (SELECT 1 AS k, 'D' AS Date) AS ll LEFT JOIN (SELECT 1 AS k) AS t1 ON ll.k = t1.k LEFT JOIN (SELECT 1 AS k) AS t2 ON ll.k = t2.k);

SELECT '=== describe: *, COLUMNS(col) -- order-independent, setting ON ===';
DESCRIBE (SELECT *, COLUMNS(Date) FROM (SELECT 1 AS k, 'D' AS Date) AS ll LEFT JOIN (SELECT 1 AS k) AS t1 ON ll.k = t1.k LEFT JOIN (SELECT 1 AS k) AS t2 ON ll.k = t2.k);

SELECT '=== describe: COLUMNS(col), * -- order-independent, setting ON ===';
DESCRIBE (SELECT COLUMNS(Date), * FROM (SELECT 1 AS k, 'D' AS Date) AS ll LEFT JOIN (SELECT 1 AS k) AS t1 ON ll.k = t1.k LEFT JOIN (SELECT 1 AS k) AS t2 ON ll.k = t2.k);

SELECT '=== outer scope cannot see the bare COLUMNS(col) name, setting ON ===';
-- fails on the old analyzer as well, since the identifier-list form never qualifies its columns
SELECT ll.Date FROM (SELECT COLUMNS(Date) FROM (SELECT 1 AS k, 'D' AS Date) AS ll LEFT JOIN (SELECT 1 AS k) AS t1 ON ll.k = t1.k LEFT JOIN (SELECT 1 AS k) AS t2 ON ll.k = t2.k); -- { serverError UNKNOWN_IDENTIFIER }

-- ============================================================
-- CTE used as a joined table expression: the qualifier is the CTE name
-- ============================================================

SET analyzer_compatibility_multiple_joins_qualify_column_names = 1;

SELECT '=== describe: CTE joined in multiple joins gets CTE-name qualifier, setting ON ===';
DESCRIBE (WITH ll AS (SELECT 1 AS k, 'D' AS Date)
          SELECT * FROM ll LEFT JOIN (SELECT 1 AS k) AS t1 ON ll.k = t1.k
                           LEFT JOIN (SELECT 1 AS k) AS t2 ON ll.k = t2.k);

SELECT '=== family2: outer ref into CTE-based derived table, setting ON ===';
SELECT ll.Date FROM (WITH ll AS (SELECT 1 AS k, 'D' AS Date)
          SELECT * FROM ll LEFT JOIN (SELECT 1 AS k) AS t1 ON ll.k = t1.k
                           LEFT JOIN (SELECT 1 AS k) AS t2 ON ll.k = t2.k);

SELECT '=== describe: UNION CTE joined in multiple joins gets CTE-name qualifier, setting ON ===';
DESCRIBE (WITH ll AS (SELECT 1 AS k, 'D' AS Date UNION ALL SELECT 2 AS k, 'E' AS Date)
          SELECT * FROM ll LEFT JOIN (SELECT 1 AS k) AS t1 ON ll.k = t1.k
                           LEFT JOIN (SELECT 1 AS k) AS t2 ON ll.k = t2.k);

SELECT '=== family2: outer ref into UNION-CTE derived table, setting ON ===';
SELECT ll.Date FROM (WITH ll AS (SELECT 1 AS k, 'D' AS Date UNION ALL SELECT 2 AS k, 'E' AS Date)
          SELECT * FROM ll LEFT JOIN (SELECT 1 AS k) AS t1 ON ll.k = t1.k
                           LEFT JOIN (SELECT 1 AS k) AS t2 ON ll.k = t2.k)
ORDER BY ll.Date;

-- ============================================================
-- Two `USING` chains: the old analyzer rejected this shape
-- (`NOT_IMPLEMENTED: Multiple USING statements are not supported`); the analyzer
-- supports it and applies the same naming as elsewhere, except that the merged
-- `USING` key belongs to the join rather than to a single table and therefore
-- keeps its bare name.
-- ============================================================

SET analyzer_compatibility_multiple_joins_qualify_column_names = 1;

SELECT '=== describe: two USING chains, setting ON ===';
DESCRIBE (SELECT * FROM (SELECT 1 AS k, 'D' AS Date) AS ll LEFT JOIN (SELECT 1 AS k) AS t1 USING (k) LEFT JOIN (SELECT 1 AS k) AS t2 USING (k));

SELECT '=== two USING chains: outer ref to a qualified column, setting ON ===';
SELECT ll.Date FROM (SELECT * FROM (SELECT 1 AS k, 'D' AS Date) AS ll LEFT JOIN (SELECT 1 AS k) AS t1 USING (k) LEFT JOIN (SELECT 1 AS k) AS t2 USING (k));

SELECT '=== two USING chains: merged key is referenced bare, setting ON ===';
SELECT k FROM (SELECT * FROM (SELECT 1 AS k, 'D' AS Date) AS ll LEFT JOIN (SELECT 1 AS k) AS t1 USING (k) LEFT JOIN (SELECT 1 AS k) AS t2 USING (k));

SELECT '=== two USING chains: merged key is not qualified, setting ON ===';
SELECT ll.k FROM (SELECT * FROM (SELECT 1 AS k, 'D' AS Date) AS ll LEFT JOIN (SELECT 1 AS k) AS t1 USING (k) LEFT JOIN (SELECT 1 AS k) AS t2 USING (k)); -- { serverError UNKNOWN_IDENTIFIER }

-- ============================================================
-- Identifier-list `COLUMNS`: a qualified item keeps the written qualifier.
-- The old analyzer's rewrite left such items spelled as written, so
-- `COLUMNS(a.x)` produced a column named `a.x` once there were two or more
-- `JOIN`s. Unqualified items are unaffected.
-- ============================================================

SET analyzer_compatibility_multiple_joins_qualify_column_names = 1;

SELECT '=== describe: COLUMNS(qualified) keeps the written name, setting ON ===';
DESCRIBE (SELECT COLUMNS(ll.Date) FROM (SELECT 1 AS k, 'D' AS Date) AS ll LEFT JOIN (SELECT 1 AS k) AS t1 ON ll.k = t1.k LEFT JOIN (SELECT 1 AS k) AS t2 ON ll.k = t2.k);

SELECT '=== family2: outer ref into a COLUMNS(qualified) derived table, setting ON ===';
SELECT ll.Date FROM (SELECT COLUMNS(ll.Date) FROM (SELECT 1 AS k, 'D' AS Date) AS ll LEFT JOIN (SELECT 1 AS k) AS t1 ON ll.k = t1.k LEFT JOIN (SELECT 1 AS k) AS t2 ON ll.k = t2.k);

SELECT '=== describe: COLUMNS(qualified, qualified) keeps both written names, setting ON ===';
DESCRIBE (SELECT COLUMNS(ll.Date, t1.k) FROM (SELECT 1 AS k, 'D' AS Date) AS ll LEFT JOIN (SELECT 1 AS k) AS t1 ON ll.k = t1.k LEFT JOIN (SELECT 1 AS k) AS t2 ON ll.k = t2.k);

SELECT '=== describe: EXCEPT still matches the bare column name, setting ON ===';
DESCRIBE (SELECT COLUMNS(ll.k, ll.Date) EXCEPT (k) FROM (SELECT 1 AS k, 'D' AS Date) AS ll LEFT JOIN (SELECT 1 AS k) AS t1 ON ll.k = t1.k LEFT JOIN (SELECT 1 AS k) AS t2 ON ll.k = t2.k);

SELECT '=== describe: COLUMNS(qualified) is untouched with the setting OFF ===';
DESCRIBE (SELECT COLUMNS(ll.Date) FROM (SELECT 1 AS k, 'D' AS Date) AS ll LEFT JOIN (SELECT 1 AS k) AS t1 ON ll.k = t1.k LEFT JOIN (SELECT 1 AS k) AS t2 ON ll.k = t2.k) SETTINGS analyzer_compatibility_multiple_joins_qualify_column_names = 0;

SELECT '=== describe: COLUMNS(qualified) is untouched at a single JOIN, setting ON ===';
DESCRIBE (SELECT COLUMNS(ll.Date) FROM (SELECT 1 AS k, 'D' AS Date) AS ll LEFT JOIN (SELECT 1 AS k) AS t1 ON ll.k = t1.k);

-- The old analyzer gives `toString(x)`/`toString(y)` here; the star and regexp
-- matcher forms already produce `toString(a.x)` under the setting, so this keeps
-- the list form consistent with them rather than adding a new deviation.
SELECT '=== describe: COLUMNS(qualified, qualified) APPLY(toString), setting ON ===';
DESCRIBE (SELECT COLUMNS(a.x, a.y) APPLY(toString) FROM (SELECT 1 AS k, 'X' AS x, 'Y' AS y) AS a LEFT JOIN (SELECT 1 AS k) AS b ON a.k = b.k LEFT JOIN (SELECT 1 AS k) AS c ON a.k = c.k);

-- The matcher must not leak its written qualifier into an unrelated sibling
-- expression referencing the same column.
SELECT '=== describe: COLUMNS(qualified), unrelated toString(qualified) does not leak the qualifier, setting ON ===';
DESCRIBE (SELECT COLUMNS(ll.Date), toString(ll.Date) FROM (SELECT 1 AS k, 'D' AS Date) AS ll LEFT JOIN (SELECT 1 AS k) AS t1 ON ll.k = t1.k LEFT JOIN (SELECT 1 AS k) AS t2 ON ll.k = t2.k);

-- The previous commit on this branch fixed matcher projection names being dropped by the
-- `group_by_use_nulls` rewrite; the clone this branch introduces must survive it too.
SELECT '=== describe: COLUMNS(qualified) with group_by_use_nulls + ROLLUP, setting ON ===';
DESCRIBE (SELECT COLUMNS(ll.Date) FROM (SELECT 1 AS k, 'D' AS Date) AS ll LEFT JOIN (SELECT 1 AS k) AS t1 ON ll.k = t1.k LEFT JOIN (SELECT 1 AS k) AS t2 ON ll.k = t2.k GROUP BY ll.Date WITH ROLLUP) SETTINGS group_by_use_nulls = 1;

SELECT '=== family2: outer ref, COLUMNS(qualified) with group_by_use_nulls + ROLLUP, setting ON ===';
SELECT ll.Date FROM (SELECT COLUMNS(ll.Date) FROM (SELECT 1 AS k, 'D' AS Date) AS ll LEFT JOIN (SELECT 1 AS k) AS t1 ON ll.k = t1.k LEFT JOIN (SELECT 1 AS k) AS t2 ON ll.k = t2.k GROUP BY ll.Date WITH ROLLUP) ORDER BY 1 NULLS LAST SETTINGS group_by_use_nulls = 1;

-- A list item that resolves through a `SELECT`-list alias also keeps the written name, which is
-- what the old analyzer did; without the setting it would be named after the underlying column.
SELECT '=== describe: COLUMNS(alias) keeps the written alias, setting ON ===';
DESCRIBE (SELECT ll.Date AS z, COLUMNS(z) FROM (SELECT 1 AS k, 'D' AS Date) AS ll LEFT JOIN (SELECT 1 AS k) AS t1 ON ll.k = t1.k LEFT JOIN (SELECT 1 AS k) AS t2 ON ll.k = t2.k);
