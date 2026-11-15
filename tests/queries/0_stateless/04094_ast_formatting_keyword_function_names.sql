-- Verify that keyword-named functions are backtick-quoted during formatting
-- so the formatted AST can be parsed back without ambiguity.
-- This is a regression test for a BuzzHouse-found bug where FROM(...) and VALUES(...)
-- lost their backtick-quoting and could not be re-parsed.

SELECT formatQuery('SELECT `FROM`(1) FROM system.one');
SELECT formatQuery('SELECT `VALUES`(1, 2)');

-- Roundtrip: format, then format again — must be identical.
SELECT formatQuery(formatQuery('SELECT `FROM`(1) FROM system.one')) = formatQuery('SELECT `FROM`(1) FROM system.one');
SELECT formatQuery(formatQuery('SELECT `VALUES`(1, 2)')) = formatQuery('SELECT `VALUES`(1, 2)');

-- Pre-existing keywords that were already handled:
SELECT formatQuery('SELECT `DISTINCT`(1)');
SELECT formatQuery('SELECT `ALL`(1)');
SELECT formatQuery('SELECT `TABLE`(1)');
