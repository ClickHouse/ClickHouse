-- Tags: no-parallel
-- no-parallel: rewrite rules are global server state

-- `{x:Int}` in a rule template is documented as matching an integer literal. Rules are applied
-- after typed query parameters were substituted, so a prepared query such as
-- `SELECT {p:Decimal32(2)}` reaches the matcher as a decimal literal (wrapped in a `_CAST` that
-- the matcher unwraps). A decimal is not an integer, so an `{x:Int}` rule must not fire for it —
-- otherwise a `REJECT` rule written for integers would also block `1.25`.

CREATE RULE rule_05054_int AS (SELECT {x:Int}) REJECT WITH 'blocked_05054';

SET query_rules = 'rule_05054_int';

-- An integer parameter is matched, as documented.
SET param_i = '42';
SELECT {i:UInt64}; -- { serverError REWRITE_RULE_REJECTION }

-- A plain integer literal is matched too.
SELECT 42; -- { serverError REWRITE_RULE_REJECTION }

-- A decimal parameter is not an integer literal and must not be matched.
SET param_p = '1.25';
SELECT {p:Decimal32(2)};

-- Not even a decimal that happens to hold a whole number: the value is still a decimal.
SET param_q = '7.00';
SELECT {q:Decimal32(2)};

SET query_rules = '';
DROP RULE rule_05054_int;
