-- Tags: no-parallel
-- no-parallel: rewrite rules are global server state

-- The deadline expression of `VALID UNTIL <datetime>` and `VALID FOR <interval>` is kept in the
-- same AST member (`valid_until` / `global_valid_until`, registered in `children`), and only the
-- `valid_until_is_interval` / `global_valid_until_is_interval` flag - kept outside `children` -
-- tells the two keywords apart. The flag is folded into the tree hash, so a template spelling
-- `VALID UNTIL <x>` must not match `VALID FOR <x>` carrying the same child expression.

DROP USER IF EXISTS user_05043;

-- Global (user-level) clause.
CREATE RULE rule_05043_global AS (CREATE USER user_05043 VALID UNTIL '2127-01-01') REJECT WITH 'blocked';
SET query_rules = 'rule_05043_global';
CREATE USER user_05043 VALID UNTIL '2127-01-01'; -- { serverError REWRITE_RULE_REJECTION }
-- The same deadline expression under `VALID FOR` must not match the `VALID UNTIL` template;
-- the query then fails `VALID FOR`'s own interval type check instead of the rule's rejection.
CREATE USER user_05043 VALID FOR '2127-01-01'; -- { serverError BAD_ARGUMENTS }
SET query_rules = '';
DROP RULE rule_05043_global;

-- Per-authentication-method clause.
CREATE RULE rule_05043_method AS (CREATE USER user_05043 IDENTIFIED WITH no_password VALID UNTIL '2127-01-01') REJECT WITH 'blocked';
SET query_rules = 'rule_05043_method';
CREATE USER user_05043 IDENTIFIED WITH no_password VALID UNTIL '2127-01-01'; -- { serverError REWRITE_RULE_REJECTION }
CREATE USER user_05043 IDENTIFIED WITH no_password VALID FOR '2127-01-01'; -- { serverError BAD_ARGUMENTS }
SET query_rules = '';
DROP RULE rule_05043_method;

-- None of the rejected statements created a user.
SELECT count() FROM system.users WHERE name = 'user_05043';
