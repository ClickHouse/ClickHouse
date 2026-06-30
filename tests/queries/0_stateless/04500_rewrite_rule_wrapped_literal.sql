-- Tags: no-parallel
-- no-parallel: rule names are global; running in parallel may collide with other tests.

-- Regression test for the different-hash wrapped-placeholder matcher path.
-- A scalar `{x:String}` placeholder that is the sole element of an `ASTExpressionList`
-- (a bare `SELECT {x:String}` projection) must match a concrete string-literal query
-- such as `SELECT 'hello'`. Previously the matcher bound the `ASTExpressionList` wrapper
-- instead of the inner literal, so the `ASTLiteral` check failed and the rule never
-- matched. Unlike `04330_rewrite_rule_wrapped_placeholder`, the incoming query here is a
-- plain literal (different subtree hash from the rule template), so it exercises the
-- different-hash branch rather than the equal-hash parameterized path.

-- String placeholder, wrapped as the sole projection.
CREATE RULE rule_wrapped_literal AS (SELECT {x:String}) REWRITE TO (SELECT {x:String}, 'rewritten');

SET query_rules = 'rule_wrapped_literal';

-- A string literal matches and is rewritten: SELECT 'hello' -> SELECT 'hello', 'rewritten'.
SELECT 'hello';
SELECT 'world';

-- A non-string literal does not match a `{x:String}` placeholder, so it is left unchanged.
SELECT 123;

SET query_rules = '';

DROP RULE rule_wrapped_literal;

-- After dropping the rule the query is no longer rewritten.
SELECT 'hello';

-- The same unwrapping applies to an `{n:Int}` placeholder.
CREATE RULE rule_wrapped_int AS (SELECT {n:Int}) REWRITE TO (SELECT {n:Int}, 'int_rewritten');

SET query_rules = 'rule_wrapped_int';

SELECT 42;

SET query_rules = '';

DROP RULE rule_wrapped_int;
