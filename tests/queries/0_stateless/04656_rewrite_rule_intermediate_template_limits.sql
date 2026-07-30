-- Tags: no-parallel
-- no-parallel: rewrite rules are global server state

-- Regression: the per-rule AST limit check that runs after every `applyRule` must also bound the
-- rule templates of an INTERMEDIATE query. A rule can rewrite a small query into rule DDL
-- (`CREATE RULE` / `ALTER RULE`), whose `source_query` / `resulting_query` templates live outside
-- `children` and outside `forEachRewriteRuleNonChildAST`. The generic post-rewrite walk does not
-- see them, but the next rule's matcher hashes them through the rule-DDL node's
-- `updateTreeHashImpl`, so without a template check here an oversized template could be walked
-- while `max_ast_elements` / `max_ast_depth` were never applied to it.

SET max_ast_depth = 1000, max_ast_elements = 50000;

-- `r_make_rule_ddl` rewrites `SELECT 41` into a `CREATE RULE` statement whose own template holds 20
-- projection columns. The rewritten `CREATE RULE` node itself is tiny outside those templates.
CREATE RULE r_make_rule_ddl AS (SELECT 41)
REWRITE TO (CREATE RULE r_inner AS (SELECT 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20) REWRITE TO (SELECT 1));

SET query_rules = 'r_make_rule_ddl';

-- With a tight element limit the submitted query (`SELECT 41`) fits, and so does the rewritten
-- `CREATE RULE` node when only `children` and the non-`children` members of the node itself are
-- counted. Only counting the rule templates rejects it. Before the fix this query ran and created
-- the inner rule.
SET max_ast_elements = 15;
SELECT 41; -- { serverError TOO_BIG_AST }

SET max_ast_depth = 1000, max_ast_elements = 50000;
SET query_rules = '';
DROP RULE r_make_rule_ddl;

-- The rejected rewrite persisted nothing.
SELECT count() FROM system.query_rules WHERE name LIKE 'r_inner%';
