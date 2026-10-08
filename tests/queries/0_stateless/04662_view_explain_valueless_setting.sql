-- `viewExplain` reparses the serialized `EXPLAIN` settings string, and must enforce the same
-- rule as `ParserExplainQuery`: `SETTINGS name` with no value is a shorthand for `= true`, and
-- `EXPLAIN` settings are read as numbers by `InterpreterExplainQuery::checkAndGetSettings` with
-- no schema to check the shorthand against, so a valueless setting must be rejected instead of
-- silently running with the setting equal to 1.

SELECT '-- A valueless integer setting is rejected';
SELECT * FROM viewExplain('EXPLAIN SYNTAX', 'query_tree_passes', (SELECT 1)); -- { serverError SYNTAX_ERROR }
SELECT 'ok';

SELECT '-- A valueless boolean setting is rejected the same way, as in `EXPLAIN` itself';
SELECT * FROM viewExplain('EXPLAIN SYNTAX', 'oneline', (SELECT 1)); -- { serverError SYNTAX_ERROR }
SELECT 'ok';

SELECT '-- With an explicit value the settings still work';
SELECT count() > 0 FROM viewExplain('EXPLAIN SYNTAX', 'oneline = 1, query_tree_passes = 1', (SELECT 1));
