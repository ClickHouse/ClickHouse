-- AI functions are beta, and `ai_function_max_retries` defaults to 1 so that a single transient
-- provider error does not fail the query. `compatibility` with an earlier version restores the
-- previous default of 0, which pins the previous_value/new_value pair in `SettingsChangesHistory`.

SELECT '-- Current default';
SELECT getSetting('ai_function_max_retries');

SELECT '-- compatibility = 26.7 restores the legacy default';
SET compatibility = '26.7';
SELECT getSetting('ai_function_max_retries');

SET compatibility = '';
