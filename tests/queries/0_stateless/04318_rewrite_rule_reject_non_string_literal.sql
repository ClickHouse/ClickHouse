-- Tags: no-parallel
-- no-parallel: rule names are global; running in parallel may collide with other tests.

SET query_rules = 1;

-- Non-string literals in `REJECT WITH` must be rejected at parse time,
-- otherwise the rule would silently become a no-op at execution.

CREATE RULE rule_non_string_create AS (SELECT 1) REJECT WITH 123; -- { clientError SYNTAX_ERROR }
CREATE RULE rule_non_string_create AS (SELECT 1) REJECT WITH NULL; -- { clientError SYNTAX_ERROR }
CREATE RULE rule_non_string_create AS (SELECT 1) REJECT WITH (1, 2); -- { clientError SYNTAX_ERROR }

CREATE RULE rule_non_string_alter AS (SELECT 1) REJECT WITH 'initial reason';

ALTER RULE rule_non_string_alter AS (SELECT 1) REJECT WITH 456; -- { clientError SYNTAX_ERROR }
ALTER RULE rule_non_string_alter AS (SELECT 1) REJECT WITH NULL; -- { clientError SYNTAX_ERROR }

-- Verify that the original rule is still in place.
SELECT 1; -- { serverError REWRITE_RULE_REJECTION }

DROP RULE rule_non_string_alter;
