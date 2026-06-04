-- Tags: no-parallel
-- no-parallel: rule names are global; running in parallel may collide with other tests.

SET query_rules = 1;

-- A result template that references a placeholder not captured by the source template
-- must be rejected at DDL time, instead of throwing for every matching query later.
CREATE RULE rule_unknown_param AS (SELECT 1) REWRITE TO (SELECT {missing:String}); -- { serverError REWRITE_RULE_UNKNOWN_QUERY_PARAMETER }

-- The same validation applies to ALTER RULE.
CREATE RULE rule_unknown_param AS (SELECT {x:String}) REWRITE TO (SELECT {x:String});
ALTER RULE rule_unknown_param AS (SELECT {x:String}) REWRITE TO (SELECT {y:String}); -- { serverError REWRITE_RULE_UNKNOWN_QUERY_PARAMETER }
DROP RULE rule_unknown_param;

-- A placeholder repeated within the source template must be rejected at DDL time,
-- instead of throwing REWRITE_RULE_DUPLICATED_QUERY_PARAMETER on matching queries.
CREATE RULE rule_dup_param AS (SELECT 1 WHERE {x:String} = {x:String}) REWRITE TO (SELECT 1); -- { serverError REWRITE_RULE_DUPLICATED_QUERY_PARAMETER }

-- A valid rule with the same placeholder used once in the source and several times in the
-- result template is accepted.
CREATE RULE rule_valid AS (SELECT {x:String}) REWRITE TO (SELECT {x:String}, {x:String});
SELECT count() FROM system.query_rules WHERE name = 'rule_valid';
DROP RULE rule_valid;
