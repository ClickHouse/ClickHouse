-- Tags: no-parallel
-- no-parallel: rewrite rules are global server state

-- A user may call the internal `_CAST` function directly (see
-- 03011_definitive_guide_to_cast.sql); such a call is part of the query's semantics and must NOT
-- be confused with the `_CAST(<literal>, '<type>')` wrapper synthesized for a typed query
-- parameter. Only the synthesized wrappers are transparent to the rewrite-rule matcher.

CREATE RULE rule_user_cast_reject AS (SELECT 1 WHERE 1 = 42) REJECT WITH 'rejected by rule_user_cast_reject';

SET query_rules = 'rule_user_cast_reject';

-- The bare literal: rejected.
SELECT 1 WHERE 1 = 42; -- { serverError REWRITE_RULE_REJECTION }

-- A typed query parameter substituting to the same value: also rejected (the synthesized
-- `_CAST` wrapper is transparent).
SET param_blocked = 42;
SELECT 1 WHERE 1 = {blocked:UInt64}; -- { serverError REWRITE_RULE_REJECTION }

-- A user-written `_CAST` call is NOT the bare literal: the cast is semantically significant
-- (e.g. `_CAST(300, 'UInt8')` wraps around), so the rule must not fire on it.
SELECT 1 WHERE 1 = _CAST(42, 'UInt64');

-- Same for a placeholder rule: `{x:Int}` captures a substituted typed parameter but not a
-- user-written `_CAST` call.
CREATE RULE rule_user_cast_capture AS (SELECT {x:Int}) REWRITE TO (SELECT {x:Int} + 100);

SET query_rules = 'rule_user_cast_capture';

-- Captured: bare literal and typed parameter.
SELECT 5;
SET param_five = 5;
SELECT {five:UInt64};

-- Not captured: the user spelled the cast, so the query runs unchanged.
SELECT _CAST(5, 'UInt64');

-- A template that spells out the same `_CAST` call still matches the user-written call exactly.
CREATE RULE rule_user_cast_exact AS (SELECT _CAST(7, 'UInt64')) REJECT WITH 'rejected by rule_user_cast_exact';

SET query_rules = 'rule_user_cast_exact';

SELECT _CAST(7, 'UInt64'); -- { serverError REWRITE_RULE_REJECTION }

-- A typed parameter substituting to `7` produces the same `_CAST(7, 'UInt64')` AST (the
-- substitution marker is internal and not part of the tree hash), so it matches the explicit
-- `_CAST` template too. The marker only makes the wrapper additionally transparent — it never
-- makes a user-written `_CAST` transparent.
SET param_seven = 7;
SELECT {seven:UInt64}; -- { serverError REWRITE_RULE_REJECTION }

SET query_rules = '';
DROP RULE rule_user_cast_reject;
DROP RULE rule_user_cast_capture;
DROP RULE rule_user_cast_exact;
