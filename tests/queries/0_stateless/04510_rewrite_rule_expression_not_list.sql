-- Tags: no-parallel
-- no-parallel: rewrite rules are global server state

-- An `{e:Expression}` placeholder captures a single expression, so a rule whose source is
-- `SELECT {e:Expression}` must NOT match a multi-expression projection such as `SELECT 1, 2`
-- (use `{l:ExpressionList}` to match a list). Otherwise the rule would capture the whole
-- `1, 2` list and rewrite multi-expression projections it was not meant to touch.

CREATE RULE rule_expr_single AS (SELECT {e:Expression}) REWRITE TO (SELECT {e:Expression}, 99);
SET query_rules = 'rule_expr_single';

-- Single expression: matches and is rewritten (adds `, 99`).
SELECT 7;
-- Multiple expressions: must NOT match, so it runs unchanged (two columns, no `99`).
SELECT 1, 2;

SET query_rules = '';
DROP RULE rule_expr_single;

-- `{l:ExpressionList}` is the placeholder for a list of expressions, so it DOES match
-- `SELECT 1, 2` and splices the captured list into the rewritten projection.
CREATE RULE rule_expr_list AS (SELECT {l:ExpressionList}) REWRITE TO (SELECT {l:ExpressionList}, 99);
SET query_rules = 'rule_expr_list';
SELECT 1, 2;

SET query_rules = '';
DROP RULE rule_expr_list;
