-- Tags: no-parallel
-- no-parallel: rewrite rules are global server state

-- `APPLY (func(params))` keeps the parameter list, and `APPLY (x -> expr)` the whole lambda,
-- outside `IAST::children` (`ASTColumnsApplyTransformer::parameters` / `lambda`), so a `{name:Type}`
-- placeholder there can be neither bound by matching nor substituted into the result. Such a
-- placeholder is rejected at `CREATE RULE` / `ALTER RULE` time instead of being stored as a rule
-- that silently never works (`forEachNonChildSemanticAST` keeps the screening, the matcher and the
-- substitution on the same carrier list).

-- Placeholder among the parameters of `APPLY func(...)`, source template.
CREATE RULE rule_04850_params AS (SELECT * APPLY quantile({q:Expression}) FROM tbl_04850) REJECT WITH 'blocked'; -- { serverError REWRITE_RULE_UNSUPPORTED_QUERY_PARAMETER_TYPE }

-- Placeholder inside the lambda of `APPLY (x -> expr)`, source template.
CREATE RULE rule_04850_lambda AS (SELECT * APPLY (x -> (x + {n:Int})) FROM tbl_04850) REJECT WITH 'blocked'; -- { serverError REWRITE_RULE_UNSUPPORTED_QUERY_PARAMETER_TYPE }

-- The result template is screened the same way.
CREATE RULE rule_04850_result AS (SELECT {n:Int}) REWRITE TO (SELECT * APPLY (x -> (x + {n:Int})) FROM tbl_04850); -- { serverError REWRITE_RULE_UNSUPPORTED_QUERY_PARAMETER_TYPE }

-- `ALTER RULE` performs the same screening.
CREATE RULE rule_04850_ok AS (SELECT 1) REWRITE TO (SELECT 2);
ALTER RULE rule_04850_ok AS (SELECT * APPLY quantile({q:Expression}) FROM tbl_04850) REJECT WITH 'blocked'; -- { serverError REWRITE_RULE_UNSUPPORTED_QUERY_PARAMETER_TYPE }
DROP RULE rule_04850_ok;

-- Templates with concrete `APPLY` transformers keep working, and the parameters / lambda subtrees
-- participate in matching: a rule for `APPLY quantile(0.9)` must not fire for `quantile(0.5)`,
-- and a rule for one lambda body must not fire for another.
DROP TABLE IF EXISTS tbl_04850;
CREATE TABLE tbl_04850 (x UInt64) ENGINE = Memory;
INSERT INTO tbl_04850 VALUES (7);

CREATE RULE rule_04850_exact_params AS (SELECT * APPLY quantile(0.9) FROM tbl_04850) REJECT WITH 'blocked';
SET query_rules = 'rule_04850_exact_params';
SELECT * APPLY quantile(0.9) FROM tbl_04850; -- { serverError REWRITE_RULE_REJECTION }
SELECT * APPLY quantile(0.5) FROM tbl_04850;
SET query_rules = '';
DROP RULE rule_04850_exact_params;

CREATE RULE rule_04850_exact_lambda AS (SELECT * APPLY (x -> (x + 1)) FROM tbl_04850) REJECT WITH 'blocked';
SET query_rules = 'rule_04850_exact_lambda';
SELECT * APPLY (x -> (x + 1)) FROM tbl_04850; -- { serverError REWRITE_RULE_REJECTION }
SELECT * APPLY (x -> (x + 2)) FROM tbl_04850;
SET query_rules = '';
DROP RULE rule_04850_exact_lambda;

DROP TABLE tbl_04850;
