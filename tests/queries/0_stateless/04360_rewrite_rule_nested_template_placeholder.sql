-- Tags: no-parallel
-- no-parallel: rule names are global; running in parallel may collide with other tests.

SET query_rules = 1;

-- A rewrite rule template can itself be a CREATE RULE / ALTER RULE statement, whose own
-- source/result templates live outside `IAST::children`. The matcher and the result
-- substitution only walk `children`, so a placeholder inside such a nested rule template
-- can be neither bound nor substituted: the rule would silently never match as intended.
-- Reject these placeholders at DDL time.

-- Placeholder inside the nested rule's source template.
CREATE RULE outer_rule AS (CREATE RULE inner_rule AS (SELECT {x:String}) REWRITE TO (SELECT 1)) REWRITE TO (SELECT 1); -- { serverError REWRITE_RULE_UNSUPPORTED_QUERY_PARAMETER_TYPE }

-- Placeholder inside the nested rule's result template.
CREATE RULE outer_rule AS (CREATE RULE inner_rule AS (SELECT 1) REWRITE TO (SELECT {x:String})) REWRITE TO (SELECT 1); -- { serverError REWRITE_RULE_UNSUPPORTED_QUERY_PARAMETER_TYPE }

-- Placeholder inside a nested rule template that sits on the result side of the outer rule.
CREATE RULE outer_rule AS (SELECT 1) REWRITE TO (CREATE RULE inner_rule AS (SELECT {x:String}) REWRITE TO (SELECT 1)); -- { serverError REWRITE_RULE_UNSUPPORTED_QUERY_PARAMETER_TYPE }

-- The same validation applies to ALTER RULE.
CREATE RULE outer_rule AS (SELECT 1) REWRITE TO (SELECT 2);
ALTER RULE outer_rule AS (CREATE RULE inner_rule AS (SELECT {x:String}) REWRITE TO (SELECT 1)) REWRITE TO (SELECT 1); -- { serverError REWRITE_RULE_UNSUPPORTED_QUERY_PARAMETER_TYPE }
DROP RULE outer_rule;

-- A nested rule template without any placeholders is still accepted: it is a literal
-- meta-rule that the matcher can compare by hash.
CREATE RULE meta_rule AS (CREATE RULE inner_rule AS (SELECT 1) REWRITE TO (SELECT 2)) REWRITE TO (SELECT 'rewritten');
SELECT count() FROM system.query_rules WHERE name = 'meta_rule';
DROP RULE meta_rule;
