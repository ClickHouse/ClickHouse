-- Test: exercises `FunctionActionName::useDefaultImplementationForLowCardinalityColumns`
-- override that the PR added but did not test. Without this override, a
-- LowCardinality(String) constant argument would be unwrapped to String before
-- reaching `getReturnTypeImpl`, silently bypassing the type check that is meant
-- to forbid non-String arguments.
-- Covers: src/Functions/identity.h:91 — useDefaultImplementationForLowCardinalityColumns() = false
-- Covers: src/Functions/identity.h:93-103 — getReturnTypeImpl type validation loop

SET enable_analyzer = 1;

-- LowCardinality(String) constant - first arg
-- Without the LowCardinality override, this would silently return 'foo'.
SELECT __actionName(toLowCardinality('foo'), 'bar'); -- { serverError BAD_ARGUMENTS }

-- LowCardinality(String) constant - second arg
SELECT __actionName('foo', toLowCardinality('bar')); -- { serverError BAD_ARGUMENTS }

-- FixedString constant - exercises getReturnTypeImpl rejection of a
-- non-String type that does NOT depend on a wrapper-unwrap override
-- (FixedString is its own type, not a wrapper around String).
SELECT __actionName(toFixedString('foo', 3), 'bar'); -- { serverError BAD_ARGUMENTS }

-- Sanity check: happy path with two DISTINCT String constants.
-- The PR's existing test uses ('aaa', 'aaa') which cannot distinguish
-- which argument is returned. This locks in that the FIRST arg is returned
-- (FunctionIdentityBase::executeImpl returns arguments.front().column).
SELECT __actionName('foo', 'bar');
