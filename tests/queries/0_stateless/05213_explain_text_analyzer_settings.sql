SET enable_analyzer = 1;

-- Both source-setting names remain untouched despite the session setting.
EXPLAIN TEXT (SELECT 1 SETTINGS enable_analyzer = 0) ONELINE;
EXPLAIN TEXT (SELECT 1 SETTINGS allow_experimental_analyzer = 0) ONELINE;

-- Outer settings govern the request without validating the preserved source.
-- The analyzer cannot be disabled, so the outer value is 1 and the conflicting 0 stays in the source.
EXPLAIN TEXT (SELECT 1 SETTINGS enable_analyzer = 0) ONELINE
SETTINGS enable_analyzer = 1;
EXPLAIN TEXT (SELECT 1 SETTINGS allow_experimental_analyzer = 0) ONELINE
SETTINGS allow_experimental_analyzer = 1;

-- Nested source settings are preserved too.
EXPLAIN TEXT (SELECT * FROM (SELECT 1 SETTINGS enable_analyzer = 0)) ONELINE;

-- Ordinary queries still reject conflicting subquery settings.
SELECT * FROM (SELECT 1 SETTINGS enable_analyzer = 0); -- { serverError INCORRECT_QUERY }
