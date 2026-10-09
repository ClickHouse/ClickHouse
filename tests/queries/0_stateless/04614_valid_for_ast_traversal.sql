-- Tags: no-parallel
-- Tag no-parallel: creates a server-global user

-- A method-level `VALID FOR` / `VALID UNTIL` deadline is an expression subtree that must be
-- visible to the generic AST machinery: query-parameter substitution must descend into it,
-- and `max_ast_depth` must count it.

DROP USER IF EXISTS user_04614_ast;

-- Query parameter inside a method-level `VALID FOR` (exercises the clone-based visitor path).
SET param_days_04614 = 2;
CREATE USER user_04614_ast IDENTIFIED WITH no_password VALID FOR INTERVAL {days_04614:UInt32} DAY;
SELECT count() FROM system.users WHERE name = 'user_04614_ast'
    AND valid_until[1] BETWEEN now() + INTERVAL 1 DAY AND now() + INTERVAL 3 DAY;
DROP USER user_04614_ast;

-- A deeply nested method-level `VALID FOR` expression must be rejected by `max_ast_depth`.
SET max_ast_depth = 20;
CREATE USER user_04614_ast IDENTIFIED WITH no_password VALID FOR INTERVAL 1 DAY + INTERVAL 1 DAY + INTERVAL 1 DAY + INTERVAL 1 DAY + INTERVAL 1 DAY + INTERVAL 1 DAY + INTERVAL 1 DAY + INTERVAL 1 DAY + INTERVAL 1 DAY + INTERVAL 1 DAY + INTERVAL 1 DAY + INTERVAL 1 DAY + INTERVAL 1 DAY + INTERVAL 1 DAY + INTERVAL 1 DAY + INTERVAL 1 DAY; -- { serverError TOO_DEEP_AST }
SET max_ast_depth = 1000;
SELECT count() FROM system.users WHERE name = 'user_04614_ast';
