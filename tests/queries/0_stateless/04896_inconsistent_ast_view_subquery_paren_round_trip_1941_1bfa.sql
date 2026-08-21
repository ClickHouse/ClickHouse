-- STID 1941-1bfa: a query carrying a trailing SETTINGS clause whose AST contains a
-- `view(SELECT ...)` table-function argument was formatted as `view((SELECT ...))`.
-- `ViewLayer` accepts only a bare select, so that text did not parse back and debug
-- and sanitizer builds hit the `Inconsistent AST formatting` LOGICAL_ERROR.
-- The trailing SETTINGS clause is the trigger.

SELECT 'fixed shapes';

-- Executing these is itself the regression assertion: the internal round-trip check
-- runs on every query.
DESCRIBE TABLE view(SELECT 1) SETTINGS input_format_orc_use_fast_decoder = 0;
EXPLAIN AST DESCRIBE TABLE view(SELECT 1) SETTINGS input_format_orc_use_fast_decoder = 0;
DESCRIBE TABLE viewIfPermitted(SELECT 1 ELSE null('x UInt8')) SETTINGS input_format_orc_use_fast_decoder = 0;

-- The formatted text must carry no parentheses around the argument, and must parse back.
SELECT formatQuerySingleLine('DESCRIBE TABLE view(SELECT 1) SETTINGS input_format_orc_use_fast_decoder = 0');
SELECT formatQuerySingleLine('EXPLAIN AST DESCRIBE TABLE view(SELECT 1) SETTINGS input_format_orc_use_fast_decoder = 0');
SELECT formatQuerySingleLine('DESCRIBE TABLE viewIfPermitted(SELECT 1 ELSE null(''x UInt8'')) SETTINGS input_format_orc_use_fast_decoder = 0');

-- The parenthesized spelling the formatter used to emit is rejected by `ViewLayer`.
SELECT formatQuerySingleLine('DESCRIBE TABLE view((SELECT 1)) SETTINGS input_format_orc_use_fast_decoder = 0'); -- { serverError SYNTAX_ERROR }

SELECT 'idempotency';

-- Formatting the formatted text must be a fixed point.
SELECT formatQuerySingleLine(formatQuerySingleLine('DESCRIBE TABLE view(SELECT 1) SETTINGS input_format_orc_use_fast_decoder = 0')) = formatQuerySingleLine('DESCRIBE TABLE view(SELECT 1) SETTINGS input_format_orc_use_fast_decoder = 0');
SELECT formatQuerySingleLine(formatQuerySingleLine('EXPLAIN AST DESCRIBE TABLE view(SELECT 1) SETTINGS input_format_orc_use_fast_decoder = 0')) = formatQuerySingleLine('EXPLAIN AST DESCRIBE TABLE view(SELECT 1) SETTINGS input_format_orc_use_fast_decoder = 0');
SELECT formatQuerySingleLine(formatQuerySingleLine('DESCRIBE TABLE viewIfPermitted(SELECT 1 ELSE null(''x UInt8'')) SETTINGS input_format_orc_use_fast_decoder = 0')) = formatQuerySingleLine('DESCRIBE TABLE viewIfPermitted(SELECT 1 ELSE null(''x UInt8'')) SETTINGS input_format_orc_use_fast_decoder = 0');

SELECT 'multi-select argument';

-- A UNION chain inside `view` also loses its branch parentheses, and both spellings parse.
SELECT formatQuerySingleLine('DESCRIBE TABLE view(SELECT 1 UNION ALL SELECT 2) SETTINGS input_format_orc_use_fast_decoder = 0');
SELECT formatQuerySingleLine(formatQuerySingleLine('DESCRIBE TABLE view(SELECT 1 UNION ALL SELECT 2) SETTINGS input_format_orc_use_fast_decoder = 0')) = formatQuerySingleLine('DESCRIBE TABLE view(SELECT 1 UNION ALL SELECT 2) SETTINGS input_format_orc_use_fast_decoder = 0');
DESCRIBE TABLE view(SELECT 1 UNION ALL SELECT 2) SETTINGS input_format_orc_use_fast_decoder = 0;

SELECT 'unaffected shapes';

-- `FORMAT` precedes SETTINGS in the output and stops the re-parser.
SELECT formatQuerySingleLine('DESCRIBE TABLE view(SELECT 1) FORMAT TSV SETTINGS input_format_orc_use_fast_decoder = 0');

-- A plain SELECT does not hoist SETTINGS above the table function.
SELECT formatQuerySingleLine('SELECT * FROM view(SELECT 1) SETTINGS input_format_orc_use_fast_decoder = 0');

SELECT 'flag still active outside the view boundary';

-- Scoping control: the individual SELECTs of a UNION chain under a trailing SETTINGS
-- clause must KEEP their parentheses, otherwise the re-parser moves SETTINGS into the
-- last SELECT.
SELECT formatQuerySingleLine('EXPLAIN (SELECT 1) UNION ALL (SELECT 2) SETTINGS max_threads = 1');
SELECT formatQuerySingleLine('EXPLAIN SYNTAX (SELECT 1) UNION (SELECT 2) SETTINGS max_threads = 1');
