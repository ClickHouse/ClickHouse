-- A window definition body is parsed twice: first as-is, then with a leading parent window name.
-- Each attempt needs its own node and the same start token, because `parseWindowDefinitionParts`
-- writes the frame and appends its offsets before it can know the brackets close, and
-- `ASTWindowDefinition::formatImpl` prints nothing for a frame carried alongside `frame_is_default`.
--
-- The error assertions below are the load-bearing part of this test: the formatted text is the same
-- before and after the fix, so only the parse outcome distinguishes them. Do not relax them into
-- format-only checks.

-- A frame ahead of the parent window name used to parse, and the frame was then dropped from the
-- query text: `formatQuerySingleLine` returned `x AS (w)`.
SELECT 1 WINDOW x AS (ROWS UNBOUNDED PRECEDING w); -- { clientError SYNTAX_ERROR }
SELECT count() OVER (GROUPS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING w); -- { clientError SYNTAX_ERROR }
SELECT 1 WINDOW x AS (ROWS 1 PRECEDING w); -- { clientError SYNTAX_ERROR }

-- PARTITION BY / ORDER BY ahead of the parent window name used to parse as the reverse order.
SELECT 1 WINDOW x AS (PARTITION BY number w); -- { clientError SYNTAX_ERROR }
SELECT 1 WINDOW x AS (ORDER BY number w); -- { clientError SYNTAX_ERROR }

-- A parent window whose name is a frame keyword: the first attempt consumes that keyword and has to
-- give it back. This is what the formatter emits for a back-quoted name, so both spellings parse and
-- formatting is a fixed point.
SELECT formatQuerySingleLine('SELECT 1 WINDOW `rows` AS (ORDER BY a), x AS (`rows`)');
SELECT formatQuerySingleLine(formatQuerySingleLine('SELECT 1 WINDOW `rows` AS (ORDER BY a), x AS (`rows`)'));
SELECT formatQuerySingleLine('SELECT 1 WINDOW w AS (ORDER BY a), x AS (rows)');
SELECT formatQuerySingleLine('SELECT 1 WINDOW w AS (ORDER BY a), x AS (range PARTITION BY a)');
SELECT formatQuerySingleLine('SELECT 1 WINDOW w AS (ORDER BY a), x AS (groups ROWS UNBOUNDED PRECEDING)');

-- Every documented spelling still parses and round-trips unchanged.
SELECT formatQuerySingleLine('SELECT 1 WINDOW w AS (ORDER BY a), x AS (w)');
SELECT formatQuerySingleLine('SELECT 1 WINDOW w AS (ORDER BY a), x AS (w ROWS UNBOUNDED PRECEDING)');
SELECT formatQuerySingleLine('SELECT 1 WINDOW w AS (ORDER BY a), x AS (w PARTITION BY a ORDER BY b ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)');
SELECT formatQuerySingleLine('SELECT 1 WINDOW w AS (ORDER BY a), x AS (PARTITION BY a)');
SELECT formatQuerySingleLine('SELECT 1 WINDOW w AS (ORDER BY a), x AS (ORDER BY a)');
SELECT formatQuerySingleLine('SELECT 1 WINDOW w AS (ORDER BY a), x AS (ROWS UNBOUNDED PRECEDING)');
SELECT formatQuerySingleLine('SELECT 1 WINDOW w AS (ORDER BY a), x AS ()');

-- The residue is invisible to `dumpTree` and to the formatter, so inside SQL the only oracle for it
-- is the round-trip hash check a debug build runs on every incoming query. These two reach it with a
-- frame-keyword parent window, the shape whose first reading consumes the keyword and fails.
SELECT 1 WINDOW `rows` AS (), x AS (`rows`);
SELECT 1 WINDOW `groups` AS (), x AS (`groups` ROWS UNBOUNDED PRECEDING);
