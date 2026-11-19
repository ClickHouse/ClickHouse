-- The `LOCAL` and `GLOBAL` keywords of a join must survive formatting. Dropping them changes
-- the query and fails the format-reparse consistency check of debug builds.
SELECT formatQuerySingleLine('SELECT count() FROM ta AS a LOCAL INNER JOIN tb AS b ON a.k = b.k');
SELECT formatQuerySingleLine('SELECT count() FROM ta AS a GLOBAL ANY LEFT JOIN tb AS b ON a.k = b.k');

-- The two statements above fill the alias slot with an explicit `AS`. The qualifier must survive
-- without one too: `LOCAL` in front of a join clause is the locality, never an implicit alias of the
-- left table. `GLOBAL` is a keyword the alias parser always refuses, so it serves as the control.

-- Parenthesized joins and pipe operators desugar into a plain SELECT whose left table has no alias,
-- which re-opens the implicit-alias slot on the second parse. `stable` is the round trip.
WITH 'SELECT * FROM (ta LOCAL CROSS JOIN tb)' AS q SELECT formatQuerySingleLine(q) = formatQuerySingleLine(formatQuerySingleLine(q)) AS stable, formatQuerySingleLine(q) AS canonical;
WITH 'SELECT * FROM (ta LOCAL INNER JOIN tb ON ta.a = tb.b)' AS q SELECT formatQuerySingleLine(q) = formatQuerySingleLine(formatQuerySingleLine(q)) AS stable, formatQuerySingleLine(q) AS canonical;
WITH 'SELECT * FROM (ta LOCAL PASTE JOIN tb)' AS q SELECT formatQuerySingleLine(q) = formatQuerySingleLine(formatQuerySingleLine(q)) AS stable, formatQuerySingleLine(q) AS canonical;
WITH 'FROM ta |> LOCAL CROSS JOIN tb' AS q SELECT formatQuerySingleLine(q) = formatQuerySingleLine(formatQuerySingleLine(q)) AS stable, formatQuerySingleLine(q) AS canonical;

-- These spellings round-trip either way, so the canonical text is the assertion: the qualifier must
-- be printed as the locality, never as `AS LOCAL`.
SELECT formatQuerySingleLine('SELECT * FROM ta LOCAL CROSS JOIN tb');
SELECT formatQuerySingleLine('SELECT * FROM ta local INNER JOIN tb ON ta.a = tb.b');
SELECT formatQuerySingleLine('SELECT * FROM ta LOCAL ANY LEFT JOIN tb ON ta.a = tb.b');
SELECT formatQuerySingleLine('SELECT * FROM (t1 CROSS JOIN t2) LOCAL CROSS JOIN t3');
-- A bare `JOIN` and a leading `NATURAL` each occupy their own position in the join grammar, so the
-- rows above reach neither. `ON` is mandatory for the bare kind, `NATURAL` needs none.
SELECT formatQuerySingleLine('SELECT * FROM ta LOCAL JOIN tb ON ta.a = tb.b');
SELECT formatQuerySingleLine('SELECT * FROM ta LOCAL NATURAL JOIN tb');

-- `GLOBAL` controls: the same two carriers must be untouched.
WITH 'SELECT * FROM (ta GLOBAL CROSS JOIN tb)' AS q SELECT formatQuerySingleLine(q) = formatQuerySingleLine(formatQuerySingleLine(q)) AS stable, formatQuerySingleLine(q) AS canonical;
WITH 'FROM ta |> GLOBAL CROSS JOIN tb' AS q SELECT formatQuerySingleLine(q) = formatQuerySingleLine(formatQuerySingleLine(q)) AS stable, formatQuerySingleLine(q) AS canonical;

-- `OUTER` is not a locality qualifier and is only ever read after a join kind, so it keeps its
-- permissive parse as an implicit alias.
SELECT formatQuerySingleLine('SELECT * FROM ta OUTER JOIN tb ON ta.a = tb.b');
SELECT formatQuerySingleLine('SELECT * FROM (ta LEFT OUTER JOIN tb ON ta.a = tb.b)');

-- Compatibility: `local` stays a valid implicit alias, column name and table name everywhere a join
-- clause cannot follow it. Each row below detects an over-broad rule. `ARRAY JOIN` is a sibling of
-- the join clause and takes an optional `LEFT`/`INNER` prefix, so all three of its spellings keep the
-- alias even though the prefix is a join keyword.
SELECT formatQuerySingleLine('SELECT 1 local');
SELECT formatQuerySingleLine('SELECT * FROM ta local');
SELECT formatQuerySingleLine('SELECT * FROM ta local WHERE local.a = 1');
SELECT formatQuerySingleLine('SELECT * FROM ta local, tb');
SELECT formatQuerySingleLine('SELECT * FROM ta local ARRAY JOIN [1] AS e');
SELECT formatQuerySingleLine('SELECT * FROM ta local LEFT ARRAY JOIN [1] AS e');
SELECT formatQuerySingleLine('SELECT * FROM ta local INNER ARRAY JOIN [1] AS e');
-- Discriminator for the two rows above: the same `LEFT` peek in front of a real join clause must take
-- the other branch, so they cannot be satisfied by dropping `LEFT`/`INNER` from the join keywords.
SELECT formatQuerySingleLine('SELECT * FROM ta local LEFT JOIN tb ON ta.a = tb.b');
SELECT formatQuerySingleLine('SELECT * FROM ta local ORDER BY local.a');
SELECT formatQuerySingleLine('SELECT 1 AS local');
SELECT formatQuerySingleLine('SELECT 1 `local`');
SELECT formatQuerySingleLine('SELECT * FROM local');
SELECT formatQuerySingleLine('SELECT local FROM ta');
-- Counter-control: `GLOBAL` is a fully reserved alias keyword and must stay a syntax error.
SELECT formatQuerySingleLine('SELECT 1 global'); -- { serverError SYNTAX_ERROR }

-- The same queries, executed: on an assertions-enabled build an unstable round trip aborts the server
-- with `Inconsistent AST formatting`, so `LOCAL` must behave exactly like `GLOBAL` here.
DROP TABLE IF EXISTS ta;
DROP TABLE IF EXISTS tb;
CREATE TABLE ta (a UInt64) ENGINE = Memory;
CREATE TABLE tb (b UInt64) ENGINE = Memory;
INSERT INTO ta VALUES (1);
INSERT INTO tb VALUES (1);
SELECT * FROM (ta LOCAL CROSS JOIN tb) ORDER BY ALL;
SELECT * FROM (ta LOCAL INNER JOIN tb ON ta.a = tb.b) ORDER BY ALL;
SELECT * FROM (ta LOCAL PASTE JOIN tb) ORDER BY ALL;
SELECT * FROM ta LOCAL CROSS JOIN tb ORDER BY ALL;
-- The pipe form wraps the left table into an aliasless subquery, which `joined_subquery_requires_alias`
-- rejects for every locality including `GLOBAL` and for no locality at all.
FROM ta |> LOCAL CROSS JOIN tb |> ORDER BY ALL SETTINGS joined_subquery_requires_alias = 0;
FROM ta |> GLOBAL CROSS JOIN tb |> ORDER BY ALL SETTINGS joined_subquery_requires_alias = 0;

-- The qualifier reaches the plan for the implicit-alias spelling too. Only the analyzer prints the
-- `Locality` line, for every spelling, so it is pinned rather than left to randomization.
SELECT count() > 0 FROM (EXPLAIN SELECT * FROM ta LOCAL INNER JOIN tb ON ta.a = tb.b) WHERE explain ILIKE '%Locality: LOCAL%' SETTINGS enable_analyzer = 1;

DROP TABLE ta;
DROP TABLE tb;
