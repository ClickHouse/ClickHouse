SELECT ignore(addDays(toDateTime(0), -1));
SELECT ignore(subtractDays(toDateTime(0), 1));

SELECT ignore(addDays(toDate(0), -1));
SELECT ignore(subtractDays(toDate(0), 1));

SET send_logs_level = 'fatal';

SELECT ignore(addDays((CAST((96.338) AS DateTime)), -3));
SELECT ignore(subtractDays((CAST((-5263074.47) AS DateTime)), -737895));
SELECT quantileDeterministic([], identity(( SELECT subtractDays((CAST((566450.398706) AS DateTime)), 54) ) )), '\0', []; -- { serverError 43 }
SELECT sequenceCount((CAST((( SELECT NULL ) AS rg, ( SELECT ( SELECT [], '<e', caseWithExpr([NULL], -588755.149, []), retention(addWeeks((CAST((-7644612.39732) AS DateTime)), -23578040.02833), (CAST(([]) AS DateTime)), (CAST(([010977.08]) AS String))), emptyArrayToSingle('') ) , '\0', toUInt64([], 't3hw@'), '\0', toStartOfQuarter(-4230.1872, []) ) ) AS Date))); -- { serverError 43 }
