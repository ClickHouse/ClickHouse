select isConstant(1);
select isConstant([1]);
select isConstant(arrayJoin([1]));
SELECT isConstant((SELECT 1));
SELECT isConstant(x) FROM (SELECT 1 x);
SELECT '---';
SELECT isConstant(x) FROM (SELECT 1 x UNION ALL SELECT 2);
SELECT '---';
select isConstant(); -- { serverError 42 }
select isConstant(1, 2); -- { serverError 42 }
