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

-- A placeholder in the source template with a type the matcher does not understand
-- (anything other than String, Int, Expression, ExpressionList, Subquery) would be
-- stored but never match a query, so it must be rejected at DDL time.
CREATE RULE rule_bad_type AS (SELECT {x:UInt64}) REWRITE TO (SELECT 'rewritten'); -- { serverError REWRITE_RULE_UNSUPPORTED_QUERY_PARAMETER_TYPE }
CREATE RULE rule_bad_type AS (SELECT {d:Date}) REWRITE TO (SELECT 'rewritten'); -- { serverError REWRITE_RULE_UNSUPPORTED_QUERY_PARAMETER_TYPE }
CREATE RULE rule_bad_type AS (SELECT {v:Nullable(String)}) REWRITE TO (SELECT 'rewritten'); -- { serverError REWRITE_RULE_UNSUPPORTED_QUERY_PARAMETER_TYPE }

-- The same validation applies to ALTER RULE.
CREATE RULE rule_bad_type AS (SELECT {x:String}) REWRITE TO (SELECT 'rewritten');
ALTER RULE rule_bad_type AS (SELECT {x:UInt64}) REWRITE TO (SELECT 'rewritten'); -- { serverError REWRITE_RULE_UNSUPPORTED_QUERY_PARAMETER_TYPE }
DROP RULE rule_bad_type;

-- A valid rule with the same placeholder used once in the source and several times in the
-- result template is accepted.
CREATE RULE rule_valid AS (SELECT {x:String}) REWRITE TO (SELECT {x:String}, {x:String});
SELECT count() FROM system.query_rules WHERE name = 'rule_valid';
DROP RULE rule_valid;

-- All supported placeholder types are accepted at DDL time.
CREATE RULE rule_types AS (SELECT {s:String}, {i:Int}, {e:Expression}, {l:ExpressionList} FROM (SELECT 1) WHERE 1 IN {q:Subquery}) REWRITE TO (SELECT {s:String});
SELECT count() FROM system.query_rules WHERE name = 'rule_types';
DROP RULE rule_types;
