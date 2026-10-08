-- A quoted identifier may lose its back-quotes on formatting only if the bare word reads back as the same
-- AST. `writeProbablyQuotedStringImpl` decides that from a hand-maintained keyword list, so a missing entry
-- makes the formatter emit text its own parser reads as syntax. The sweep below derives its cases from
-- `system.keywords` rather than naming them, so the next gap in that list reddens this test.
-- The cases only format: the consistency check in `executeQuery` compares a parsed AST with a re-parse of
-- its own output, so a regressed formatter would abort a debug server on the offending shape instead of
-- reddening one line.
-- Expected output: the guard line only.

WITH roundtrip AS
(
    SELECT
        keyword,
        arrayJoin([
            'SELECT NOT `{}`',
            'SELECT 1 AND `{}`',
            'SELECT `{}` FROM t',
            'SELECT `{}`, 1',
            'SELECT -`{}`',
            'SELECT `{}` + 1',
            'SELECT `{}`(1)',
            'SELECT `{}`()',
            'SELECT `{}`(1, 2)',
            'SELECT 1 AS `{}`',
            'SELECT `{}`.x FROM t AS `{}`',
            'SELECT * FROM t WHERE `{}`',
            'SELECT * FROM t GROUP BY `{}`',
            'SELECT * FROM t GROUP BY `{}` WITH TOTALS',
            'SELECT * FROM t ORDER BY `{}`',
            'SELECT * FROM t HAVING `{}`',
            'SELECT x FROM `{}`',
            'SELECT x FROM `{}`.t',
            'ALTER TABLE t ADD COLUMN `{}` Int32',
            'CREATE TABLE t (`{}` Int32) ENGINE = MergeTree ORDER BY `{}`',
            'CREATE VIEW v AS SELECT `{}` + 1 FROM t',
            'WITH 1 AS `{}` SELECT `{}`'
        ]) AS tpl,
        formatQuerySingleLineOrNull(replaceAll(tpl, '{}', keyword)) AS formatted,
        formatQuerySingleLineOrNull(formatted) AS reformatted
    FROM system.keywords
)
SELECT * FROM
(
    SELECT concat(keyword, ' | ', tpl, ' | ', formatted, ' | ', ifNull(reformatted, '<cannot parse back>')) AS result
    FROM roundtrip
    WHERE formatted IS NOT NULL AND (reformatted IS NULL OR reformatted != formatted)
    UNION ALL
    -- An empty failure list also describes a sweep that examined nothing, so the extent is asserted too, and
    -- computed rather than written down. A few inputs cannot parse even quoted, because `CAST` and `EXISTS`
    -- have dedicated parsers that do not accept a back-quoted name in front of `(`; those are excluded by
    -- the filter above, so the share that does parse is asserted instead of a fixed count.
    SELECT concat(
        'guard: every_keyword_examined=', toString(uniqExact(keyword) = (SELECT count() FROM system.keywords)),
        ' positions=', toString(uniqExact(tpl)),
        ' nearly_all_inputs_parse=', toString(countIf(formatted IS NOT NULL) >= ((count() * 99) DIV 100))) AS result
    FROM roundtrip
)
ORDER BY result;

-- The three names the sweep found, now kept quoted.
SELECT formatQuerySingleLine('SELECT NOT `not`');
SELECT formatQuerySingleLine('SELECT * FROM t GROUP BY `cube`');
SELECT formatQuerySingleLine('SELECT * FROM t GROUP BY `rollup`');

-- A stored definition carries the formatter's own text, so an unquoted name changes what it means on re-read.
SELECT formatQuerySingleLine('CREATE TABLE t (`not` Int32) ENGINE = MergeTree ORDER BY `not`');
SELECT formatQuerySingleLine('CREATE VIEW v AS SELECT `not` + 1 FROM t');

-- Controls. `NOT` as an operator is rendered from the node, not from a name, so the high-traffic spellings
-- are unaffected.
SELECT formatQuerySingleLine('SELECT NOT x');
SELECT formatQuerySingleLine('SELECT not(x)');
SELECT formatQuerySingleLine('SELECT 1 NOT IN (1)');
SELECT formatQuerySingleLine('SELECT 1 IS NOT NULL');

-- The grouping-set modifiers are literal keyword text rather than identifiers, and the prefix form is
-- normalised to the suffix form, so neither spelling gains back-quotes.
SELECT formatQuerySingleLine('SELECT count() FROM t GROUP BY a WITH ROLLUP');
SELECT formatQuerySingleLine('SELECT count() FROM t GROUP BY CUBE(a, b)');

-- Quoting is by exact name, in any case; a name that merely starts like one is left alone.
SELECT formatQuerySingleLine('SELECT `NOT` + 1');
SELECT formatQuerySingleLine('SELECT notEmpty(x)');
SELECT formatQuerySingleLine('SELECT cubeRoot(1)');
