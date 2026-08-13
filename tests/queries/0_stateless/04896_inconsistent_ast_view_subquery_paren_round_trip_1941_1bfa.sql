-- STID 1941-1bfa: a query carrying a trailing SETTINGS clause whose AST contains a
-- `view(SELECT ...)` table-function argument was formatted as `view((SELECT ...))`.
-- `ParserViewLayer` accepts only a bare select, so the formatted text did not parse
-- back and debug / sanitizer builds hit the `Inconsistent AST formatting`
-- LOGICAL_ERROR.
--
-- `ASTQueryWithOutput::formatImpl` sets `parent_has_trailing_settings` so that an
-- inner `ASTSelectWithUnionQuery` parenthesizes its individual SELECTs, keeping the
-- re-parser from consuming the trailing SETTINGS into the last SELECT. The flag was
-- inherited into the `view` / `viewIfPermitted` argument, where the closing paren of
-- the table function already terminates the select, so the parentheses were both
-- redundant and rejected on re-parse. The fix clears the flag when crossing that
-- argument boundary, matching the resets in `ASTSubquery`, `ASTCreateQuery` and
-- `ASTAlterQuery`.
--
-- The trailing SETTINGS clause is the trigger: without it nothing sets the flag and
-- the output is already correct.

SELECT 'fixed shapes';

-- Each of these three aborted with LOGICAL_ERROR before the fix. Executing them is
-- itself the regression assertion: the internal round-trip check runs on every query.
DESCRIBE TABLE view(SELECT 1) SETTINGS input_format_orc_use_fast_decoder = 0;
EXPLAIN AST DESCRIBE TABLE view(SELECT 1) SETTINGS input_format_orc_use_fast_decoder = 0;
DESCRIBE TABLE viewIfPermitted(SELECT 1 ELSE null('x UInt8')) SETTINGS input_format_orc_use_fast_decoder = 0;

-- The formatted text must carry no parentheses around the argument, and must parse back.
SELECT formatQuerySingleLine('DESCRIBE TABLE view(SELECT 1) SETTINGS input_format_orc_use_fast_decoder = 0');
SELECT formatQuerySingleLine('EXPLAIN AST DESCRIBE TABLE view(SELECT 1) SETTINGS input_format_orc_use_fast_decoder = 0');
SELECT formatQuerySingleLine('DESCRIBE TABLE viewIfPermitted(SELECT 1 ELSE null(''x UInt8'')) SETTINGS input_format_orc_use_fast_decoder = 0');

-- The parenthesized spelling the formatter used to emit is not accepted by
-- `ParserViewLayer`, which is why the round trip diverged.
SELECT formatQuerySingleLine('DESCRIBE TABLE view((SELECT 1)) SETTINGS input_format_orc_use_fast_decoder = 0'); -- { serverError SYNTAX_ERROR }

SELECT 'idempotency';

-- Formatting the formatted text must be a fixed point.
SELECT formatQuerySingleLine(formatQuerySingleLine('DESCRIBE TABLE view(SELECT 1) SETTINGS input_format_orc_use_fast_decoder = 0')) = formatQuerySingleLine('DESCRIBE TABLE view(SELECT 1) SETTINGS input_format_orc_use_fast_decoder = 0');
SELECT formatQuerySingleLine(formatQuerySingleLine('EXPLAIN AST DESCRIBE TABLE view(SELECT 1) SETTINGS input_format_orc_use_fast_decoder = 0')) = formatQuerySingleLine('EXPLAIN AST DESCRIBE TABLE view(SELECT 1) SETTINGS input_format_orc_use_fast_decoder = 0');
SELECT formatQuerySingleLine(formatQuerySingleLine('DESCRIBE TABLE viewIfPermitted(SELECT 1 ELSE null(''x UInt8'')) SETTINGS input_format_orc_use_fast_decoder = 0')) = formatQuerySingleLine('DESCRIBE TABLE viewIfPermitted(SELECT 1 ELSE null(''x UInt8'')) SETTINGS input_format_orc_use_fast_decoder = 0');

SELECT 'multi-select argument';

-- A multi-select UNION chain inside `view` also loses its branch parentheses, because
-- the closing paren of the table function already stops the trailing SETTINGS from
-- being absorbed. Both spellings parse; this pins the current one.
SELECT formatQuerySingleLine('DESCRIBE TABLE view(SELECT 1 UNION ALL SELECT 2) SETTINGS input_format_orc_use_fast_decoder = 0');
SELECT formatQuerySingleLine(formatQuerySingleLine('DESCRIBE TABLE view(SELECT 1 UNION ALL SELECT 2) SETTINGS input_format_orc_use_fast_decoder = 0')) = formatQuerySingleLine('DESCRIBE TABLE view(SELECT 1 UNION ALL SELECT 2) SETTINGS input_format_orc_use_fast_decoder = 0');
DESCRIBE TABLE view(SELECT 1 UNION ALL SELECT 2) SETTINGS input_format_orc_use_fast_decoder = 0;

SELECT 'unaffected shapes';

-- `FORMAT` precedes SETTINGS in the output and stops the re-parser, so the setter
-- never raises the flag.
SELECT formatQuerySingleLine('DESCRIBE TABLE view(SELECT 1) FORMAT TSV SETTINGS input_format_orc_use_fast_decoder = 0');

-- A plain SELECT is not an `ASTQueryWithOutput` ancestor that hoists SETTINGS above
-- the table function, so the argument was never parenthesized here.
SELECT formatQuerySingleLine('SELECT * FROM view(SELECT 1) SETTINGS input_format_orc_use_fast_decoder = 0');

SELECT 'flag still active outside the view boundary';

-- The reset must be scoped to the `view` argument. These are the shapes
-- `parent_has_trailing_settings` exists for (see 04039): the individual SELECTs of a
-- UNION chain under a trailing SETTINGS clause must KEEP their parentheses, otherwise
-- the re-parser moves SETTINGS into the last SELECT.
SELECT formatQuerySingleLine('EXPLAIN (SELECT 1) UNION ALL (SELECT 2) SETTINGS max_threads = 1');
SELECT formatQuerySingleLine('EXPLAIN SYNTAX (SELECT 1) UNION (SELECT 2) SETTINGS max_threads = 1');
