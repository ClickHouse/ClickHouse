-- Tags: no-parallel
-- no-parallel: rule names are global; running in parallel may collide with other tests.

-- Regression test: when a scalar `{x:String}` placeholder is the sole element of an
-- `ASTExpressionList` (e.g. a bare select expression) and both the rule source and the
-- incoming query are parameterized with it, matching previously bound the whole
-- `ASTExpressionList` wrapper instead of the inner node, substituting an extra nesting
-- layer into the result template. The binding must capture the inner value.

SET query_rules = 1;

CREATE RULE rule_wrapped AS (SELECT {x:String}) REWRITE TO (SELECT {x:String}, 'rewritten');

SET param_x = 'hello';
SELECT {x:String};

DROP RULE rule_wrapped;
