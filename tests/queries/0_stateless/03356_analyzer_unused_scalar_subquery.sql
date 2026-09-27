-- An unused scalar subquery or an unused `WITH` expression must not be evaluated,
-- so `throwIf` never fires. This is checked with `throwIf` rather than with a
-- timeout, because a timeout makes the test flaky on a loaded machine.

set enable_analyzer = 1;

WITH (
        SELECT throwIf(1)
    ) AS res
SELECT *
FROM system.one
FORMAT Null;

WITH throwIf(1) AS res
SELECT *
FROM system.one
FORMAT Null;

-- But it is evaluated when it is actually used.

WITH (
        SELECT throwIf(1)
    ) AS res
SELECT res
FROM system.one
FORMAT Null; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }

WITH throwIf(1) AS res
SELECT res
FROM system.one
FORMAT Null; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }
