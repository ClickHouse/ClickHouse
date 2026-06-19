-- Tags: no-parallel
-- no-parallel: rule names are global; running in parallel may collide with other tests.

SET query_rules = 1;

-- The result template is validated at DDL time too, not just the source template. A
-- placeholder in the result whose type the substitution does not understand (anything other
-- than String, Int, Expression, ExpressionList, Subquery) is rejected up front, instead of
-- producing a malformed AST when the rule later matches a query.

-- Unsupported scalar type in the result.
CREATE RULE rule_result_bad_type AS (SELECT {x:String}) REWRITE TO (SELECT {x:UInt64}); -- { serverError REWRITE_RULE_UNSUPPORTED_QUERY_PARAMETER_TYPE }

-- The reviewer's example: an Identifier placeholder in the result is not supported and must
-- be rejected rather than substituting a captured literal into a table-identifier position.
CREATE RULE rule_result_identifier AS (SELECT {t:String}) REWRITE TO (SELECT * FROM {t:Identifier}); -- { serverError REWRITE_RULE_UNSUPPORTED_QUERY_PARAMETER_TYPE }

-- A placeholder reused in the result with a different (but individually supported) type than
-- the source is rejected: substitution binds the capture by name and ignores the result-side
-- type, so the captured String literal would land in the `{x:Int}` position.
CREATE RULE rule_result_type_mismatch AS (SELECT {x:String}) REWRITE TO (SELECT number FROM numbers({x:Int})); -- { serverError REWRITE_RULE_UNSUPPORTED_QUERY_PARAMETER_TYPE }

-- The same validation applies to ALTER RULE.
CREATE RULE rule_result_alter AS (SELECT {x:String}) REWRITE TO (SELECT {x:String});
ALTER RULE rule_result_alter AS (SELECT {x:String}) REWRITE TO (SELECT {x:UInt64}); -- { serverError REWRITE_RULE_UNSUPPORTED_QUERY_PARAMETER_TYPE }
DROP RULE rule_result_alter;

-- A result template using only supported placeholder types is accepted.
CREATE RULE rule_result_ok AS (SELECT {x:String}) REWRITE TO (SELECT {x:String}, {x:String});
SELECT count() FROM system.query_rules WHERE name = 'rule_result_ok';
DROP RULE rule_result_ok;
