-- Tags: no-parallel
-- no-parallel: rewrite rules are global server state

-- The matcher captures an `{l:ExpressionList}` placeholder only when it stands for a whole list.
-- Next to other items of the same list in the source template it could never match, so such a
-- rule is rejected at `CREATE RULE` / `ALTER RULE` time.

CREATE RULE rule_05332_projection AS (SELECT 0, {l:ExpressionList}) REJECT WITH 'blocked_05332'; -- { serverError REWRITE_RULE_UNSUPPORTED_QUERY_PARAMETER_TYPE }
CREATE RULE rule_05332_projection AS (SELECT {l:ExpressionList}, 0) REJECT WITH 'blocked_05332'; -- { serverError REWRITE_RULE_UNSUPPORTED_QUERY_PARAMETER_TYPE }
CREATE RULE rule_05332_arguments AS (SELECT f(1, {l:ExpressionList})) REJECT WITH 'blocked_05332'; -- { serverError REWRITE_RULE_UNSUPPORTED_QUERY_PARAMETER_TYPE }

-- The whole list is accepted, as the projection and as the arguments of a function, and matches.
CREATE RULE rule_05332_whole AS (SELECT greatest({l:ExpressionList})) REJECT WITH 'blocked_05332';
ALTER RULE rule_05332_whole AS (SELECT greatest(0, {l:ExpressionList})) REJECT WITH 'blocked_05332'; -- { serverError REWRITE_RULE_UNSUPPORTED_QUERY_PARAMETER_TYPE }

SET query_rules = 'rule_05332_whole';
SELECT greatest(1, 2); -- { serverError REWRITE_RULE_REJECTION }
SELECT least(1, 2);
SET query_rules = '';

-- A result template may put a captured list next to other items.
DROP RULE rule_05332_whole;
CREATE RULE rule_05332_whole AS (SELECT {l:ExpressionList}) REWRITE TO (SELECT 0, {l:ExpressionList});
SET query_rules = 'rule_05332_whole';
SELECT 1, 2;
SET query_rules = '';

DROP RULE rule_05332_whole;
