-- Tags: no-parallel
-- no-parallel: rule names are global; running in parallel may collide with other tests.

SET query_rules = 1;

-- A result template can place a captured placeholder directly into an AST field that is also
-- referenced by a typed member pointer, such as `ASTTableJoin::on_expression` in a
-- `JOIN ... ON {cond}`. The substitution must update that typed pointer, otherwise the
-- rewritten query is analyzed with the stale `ASTQueryParameter` and fails. Rewrite a
-- `JOIN ... ON {cond}` rule and check the rewritten query executes and returns rows.
CREATE RULE rule_join_on
    AS (SELECT count() FROM numbers(3) AS a INNER JOIN numbers(3) AS b ON {cond:Expression})
    REWRITE TO (SELECT count() FROM numbers(3) AS a INNER JOIN numbers(3) AS b ON {cond:Expression});

-- Matches the rule, capturing `a.number = b.number` as {cond}; the rewrite must substitute it
-- into the JOIN ON of the result. There are 3 equal pairs, so the count is 3.
SELECT count() FROM numbers(3) AS a INNER JOIN numbers(3) AS b ON a.number = b.number;

DROP RULE rule_join_on;
