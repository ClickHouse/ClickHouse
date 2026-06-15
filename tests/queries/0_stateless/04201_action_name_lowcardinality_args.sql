-- Test: exercises `FunctionActionName::useDefaultImplementationForLowCardinalityColumns`
-- override. Without this override, a LowCardinality(String) constant second
-- argument would be unwrapped to String before reaching `getReturnTypeImpl`,
-- silently bypassing the type check on the action name argument.
-- The first argument can be any type (it is the wrapped expression).

SET enable_analyzer = 1;

-- First arg can be any type — it is the expression being wrapped.
SELECT __actionName(toLowCardinality('foo'), 'bar');
SELECT __actionName(toFixedString('foo', 3), 'bar');
SELECT __actionName(42, 'bar');

-- Second arg must be String — it is the action name.
SELECT __actionName('foo', toLowCardinality('bar')); -- { serverError BAD_ARGUMENTS }
SELECT __actionName('foo', 42); -- { serverError BAD_ARGUMENTS }

-- Sanity check: the function returns its first argument unchanged.
SELECT __actionName('foo', 'bar');
