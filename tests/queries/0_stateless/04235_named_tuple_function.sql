SELECT toTypeName(namedTuple(1 AS a, 'x' AS b));
SELECT toJSONString(namedTuple(1 AS a, 'x' AS b));

SELECT toTypeName(tuple(1 AS a, 'x' AS b));
SELECT toJSONString(tuple(1 AS a, 'x' AS b));

SELECT toTypeName(namedTuple());
SELECT toJSONString(namedTuple());

SELECT namedTuple(1, 2); -- { serverError BAD_ARGUMENTS }
SELECT namedTuple(1 AS a, 2 AS a); -- { serverError BAD_ARGUMENTS }
SELECT toJSONString(namedTuple(1 AS a, 2 AS a)); -- { serverError BAD_ARGUMENTS }
SELECT namedTuple(1 AS `not valid`, 2 AS b); -- { serverError BAD_ARGUMENTS }
