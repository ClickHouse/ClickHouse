-- Regression test for the analyzer honoring `SETTINGS x = DEFAULT` as the ONLY setting of an inner
-- subquery. `SETTINGS x = DEFAULT` is parsed into `ASTSetQuery::default_settings`, not `changes`, so the
-- `QueryNode` has no recorded `settings_changes` and `hasSettingsChanges()` is false. The analyzer must
-- still execute a table function inside such a subquery with the reset settings (the reset is applied to
-- the subquery's context), not with the outer/session settings.
--
-- Note: this is intentionally distinct from `04407_obfuscate_inner_settings_default_reset`, whose inner
-- `SETTINGS` also sets `obfuscate_seed = 'stable'`, making `hasSettingsChanges()` true and thereby
-- bypassing the DEFAULT-only path exercised here.

SET allow_experimental_analyzer = 1;
SET obfuscate_markov_order = 0;

-- The inner subquery's only setting is a DEFAULT reset (no `SETTINGS x = value`). The invalid session
-- value 0 is reset back to the default 5, so the obfuscation succeeds and returns 8 rows. Before the fix
-- the table function still saw the session value 0 and threw `BAD_ARGUMENTS`.
SELECT count() FROM (
    SELECT * FROM obfuscate(SELECT 'hello world' AS s FROM numbers(8)) LIMIT 8
    SETTINGS obfuscate_markov_order = DEFAULT);

-- Sanity check: with no inner settings the invalid session value still applies and the query fails.
SELECT count() FROM (
    SELECT * FROM obfuscate(SELECT 'hello world' AS s FROM numbers(8)) LIMIT 8); -- { serverError BAD_ARGUMENTS }
