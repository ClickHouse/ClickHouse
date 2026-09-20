-- A name the formatter emits bare must re-parse as that same name. Three sites dropped the
-- back-quotes, so `Inconsistent AST formatting` LOGICAL_ERROR aborted debug and sanitizer builds,
-- and a release build (where that check is compiled out) persisted the unreadable text into
-- stored metadata:
--   * a function named `COLUMNS` was re-read as the column matcher (`ParserColumnsMatcher`),
--   * `EXCEPT `strict`` was re-read as the transformer's own STRICT modifier,
--   * a window function named `not` was printed as the `NOT` operator, which cannot carry `OVER`.
--
-- A bare `not` in the *name* position is quoted by the global force-quote list in
-- `writeProbablyQuotedStringImpl`, not by the sites above, so `SELECT `not`(x) IGNORE NULLS`,
-- `SELECT `not`(x) RESPECT NULLS` and `SELECT `not`(x)(1)` still abort on this base and are
-- covered by https://github.com/ClickHouse/ClickHouse/pull/121054 instead. They are deliberately
-- absent here; the sweep at the end asserts they are the only remaining shapes.

DROP TABLE IF EXISTS t_05245;
CREATE TABLE t_05245 (a UInt8, b UInt8, x UInt8, y UInt8, `strict` UInt8) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_05245 VALUES (1, 2, 3, 4, 5);

-- The emitted text keeps the quotes. These hold in every build type, including release, where the
-- internal round-trip check is compiled out and only the stored text is left broken.
SELECT formatQuerySingleLine('SELECT `COLUMNS`(x) FROM t');
SELECT formatQuerySingleLine('SELECT `COLUMNS`(x) IGNORE NULLS FROM t');
SELECT formatQuerySingleLine('SELECT `COLUMNS`(x) OVER () FROM t');
SELECT formatQuerySingleLine('SELECT `COLUMNS`(x)(1) FROM t');
SELECT formatQuerySingleLine('SELECT `columns`(x) FROM t');
SELECT formatQuerySingleLine('SELECT * EXCEPT `strict` FROM t');
SELECT formatQuerySingleLine('SELECT * EXCEPT (`strict`) FROM t');
SELECT formatQuerySingleLine('SELECT * EXCEPT `STRICT` FROM t');

-- The name also survives as a FUNCTION name rather than becoming a matcher: an unknown function is
-- reported instead of a column being returned. Without the quotes these shapes either abort or,
-- for the suffix-free ones, silently resolve to a column.
SELECT `COLUMNS`(x) FROM t_05245; -- { serverError UNKNOWN_FUNCTION }
SELECT `COLUMNS`(x, y) FROM t_05245; -- { serverError UNKNOWN_FUNCTION }
SELECT `COLUMNS`(t_05245.x) FROM t_05245; -- { serverError UNKNOWN_FUNCTION }
SELECT `COLUMNS`('a.*') FROM t_05245; -- { serverError UNKNOWN_FUNCTION }
SELECT `COLUMNS`(x) IGNORE NULLS FROM t_05245; -- { serverError UNKNOWN_FUNCTION }
SELECT `COLUMNS`(x) RESPECT NULLS FROM t_05245; -- { serverError UNKNOWN_FUNCTION }
SELECT `COLUMNS`(x) AS c FROM t_05245; -- { serverError UNKNOWN_FUNCTION }
SELECT `CoLuMnS`(x) FROM t_05245; -- { serverError UNKNOWN_FUNCTION }
SELECT `COLUMNS`(x) OVER () FROM t_05245; -- { serverError UNKNOWN_AGGREGATE_FUNCTION }
SELECT `COLUMNS`(x) OVER (PARTITION BY y) FROM t_05245; -- { serverError UNKNOWN_AGGREGATE_FUNCTION }
SELECT `COLUMNS`(x) OVER w FROM t_05245 WINDOW w AS (); -- { serverError UNKNOWN_AGGREGATE_FUNCTION }
SELECT `COLUMNS`(x)(1) FROM t_05245; -- { serverError BAD_ARGUMENTS }

-- A column named `strict` can be excluded, parenthesized or not.
SELECT * EXCEPT `strict` FROM t_05245;
SELECT * EXCEPT (`strict`) FROM t_05245;

-- ... and such a definition survives a round trip through stored metadata, which is the only
-- consequence a release build exhibits: ATTACH re-reads the text the formatter wrote.
DROP VIEW IF EXISTS v_05245;
CREATE VIEW v_05245 AS SELECT * EXCEPT `strict` FROM t_05245;
DETACH TABLE v_05245;
ATTACH TABLE v_05245;
SELECT * FROM v_05245;
DROP VIEW v_05245;

-- The operator form has nowhere to put a window specification. The parser sets `is_operator` only
-- for `not`, but it is readable from SQL for any function through the AST JSON, and `plus` is not a
-- keyword, so no amount of quoting reaches this shape: only the `isWindowFunction()` guard does.
WITH parseQueryToJSON('SELECT plus(x, y) OVER ()') AS j
SELECT formatQueryFromJSON(replaceOne(j, '"name":"plus"', '"name":"plus","is_operator":true'));

-- Shapes that must keep working. An idiomatic `COLUMNS(...)` is a different node type, printed from
-- a hard-coded string, and must stay unquoted; so must a qualified matcher.
SELECT COLUMNS(x) FROM t_05245;
SELECT COLUMNS('^[ab]$') FROM t_05245;
SELECT `t_05245`.`COLUMNS`(x) FROM t_05245;
SELECT * EXCEPT STRICT `strict` FROM t_05245;
SELECT * EXCEPT (`strict`, a) FROM t_05245;
SELECT * EXCEPT ('str.*') FROM t_05245;
SELECT `COLUMNS`(*) FROM t_05245; -- { serverError UNKNOWN_FUNCTION }

-- `not` in the operator position still prints bare, so the existing `not(...)` references do not
-- move. If this starts printing `` `not` ``, the window guard has leaked into the context where
-- operators are disabled.
EXPLAIN SYNTAX SELECT NOT true;

-- A derived sweep over every keyword in the four affected positions, so a further keyword cannot
-- land silently. The oracle is textual, and therefore blind to the silent matcher substitution the
-- enumerated rows above cover; it is not a substitute for them. `NOT` in the name positions is the
-- known residual owned by #121054, so the assertion is "nothing else is broken" and stays green
-- either way once that PR merges.
WITH
    ['SELECT `@`(x) IGNORE NULLS FROM t', 'SELECT `@`(x) OVER () FROM t',
     'SELECT `@`(x)(1) FROM t', 'SELECT * EXCEPT `@` FROM t'] AS templates,
    probes AS
    (
        SELECT keyword, arrayJoin(arrayEnumerate(templates)) AS pos,
               replaceAll(templates[pos], '@', keyword) AS q
        FROM system.keywords
    ),
    parsed AS (SELECT keyword, pos, formatQuerySingleLineOrNull(q) AS f1 FROM probes)
SELECT
    countIf(f1 IS NOT NULL) >= 2000 AS sweep_covers_enough_probes,
    arrayFilter(x -> NOT startsWith(x, 'NOT @'),
        arraySort(groupArrayIf(concat(keyword, ' @', toString(pos)),
            f1 IS NOT NULL AND formatQuerySingleLineOrNull(f1) IS NULL))) AS unexpected_failures
FROM parsed;

DROP TABLE t_05245;
