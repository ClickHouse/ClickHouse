-- A query-construction setting reset to DEFAULT (`SETTINGS limit = DEFAULT`) lives in the AST's
-- `default_settings`, not `changes`. The per-`UNION`-arm construction-settings pass must recognize it
-- there too, both for per-arm-mode detection and for the last-arm ambiguity rejection. Otherwise a
-- mixed `(… SETTINGS limit = 1) UNION ALL … SETTINGS limit = DEFAULT` would be accepted and silently
-- re-scoped to the whole union instead of being rejected as ambiguous.
-- Companion of 04338_query_construction_settings_subqueries.

SELECT '-- last-arm `= DEFAULT` is detected: the ambiguous mixed union is rejected';
SELECT count() FROM ((SELECT number FROM numbers(100) SETTINGS limit = 1) UNION ALL (SELECT number FROM numbers(100) SETTINGS limit = DEFAULT)); -- { serverError BAD_ARGUMENTS }

SELECT '-- non-last-arm `= DEFAULT` also enables per-arm mode, so SETTINGS on the last arm is ambiguous';
SELECT count() FROM ((SELECT number FROM numbers(100) SETTINGS limit = DEFAULT) UNION ALL (SELECT number FROM numbers(100) SETTINGS limit = 2)); -- { serverError BAD_ARGUMENTS }

SELECT '-- nesting each arm in a subquery keeps `= DEFAULT` unambiguous (1 + 3 = 4)';
SELECT count() FROM (SELECT * FROM (SELECT number FROM numbers(3) SETTINGS limit = 1) UNION ALL SELECT * FROM (SELECT number FROM numbers(3) SETTINGS limit = DEFAULT));
