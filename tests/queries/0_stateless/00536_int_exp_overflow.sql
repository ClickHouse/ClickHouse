SELECT 'intExp2:';
SELECT arrayJoin([-inf, -1000.5, -1000, -2.5, -2, -1.5, -1, -0.5, 0, 0.5, 1, 1.5, 2, 2.5, 62, 62.5, 63, 63.5, 64, 64.5, 65, 65.5, 1000, 1000.5, inf]) as x, toTypeName(x), intExp2(x);
SELECT arrayJoin([-1000, -2, -1, 0, 1, 2, 62, 63, 64, 65, 1000]) as x, toTypeName(x), intExp2(x);
SELECT intExp2(nan); -- { serverError BAD_ARGUMENTS }

SELECT 'intExp10:';
SELECT arrayJoin([-inf, -1000.5, -1000, -2.5, -2, -1.5, -1, -0.5, 0, 0.5, 1, 1.5, 2, 2.5, 18, 18.5, 19, 19.5, 20, 20.5, 21, 21.5, 1000, 1000.5, inf]) as x, toTypeName(x), intExp10(x);
SELECT arrayJoin([-1000, -2, -1, 0, 1, 2, 18, 19, 20, 21, 1000]) as x, toTypeName(x), intExp10(x);
SELECT intExp10(nan); -- { serverError BAD_ARGUMENTS }
