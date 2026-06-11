-- Test: exercises `FunctionActionName` argument type validation. The first
-- argument (the expression whose action name is overridden) may have any type
-- and is returned unchanged. The second argument is the name itself and must
-- be a genuine String: `useDefaultImplementationForLowCardinalityColumns` and
-- `useDefaultImplementationForNulls` are disabled, so a wrapper type like
-- LowCardinality(String) or Nullable(String) is not silently unwrapped before
-- reaching `getReturnTypeImpl` and is rejected.
-- Covers: src/Functions/identity.h — FunctionActionName::getReturnTypeImpl

SET enable_analyzer = 1;

-- LowCardinality(String) first arg - allowed, returned unchanged.
SELECT __actionName(toLowCardinality('foo'), 'bar');

-- LowCardinality(String) second arg - the name must be a genuine String.
SELECT __actionName('foo', toLowCardinality('bar')); -- { serverError BAD_ARGUMENTS }

-- FixedString first arg - allowed, returned unchanged.
SELECT __actionName(toFixedString('foo', 3), 'bar');

-- Sanity check: happy path with two DISTINCT String constants.
-- This locks in that the FIRST arg is returned
-- (FunctionIdentityBase::executeImpl returns arguments.front().column).
SELECT __actionName('foo', 'bar');
