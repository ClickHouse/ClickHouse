-- https://github.com/ClickHouse/ClickHouse/issues/93026
SELECT hex(groupConcatMerge(',', 10)(state))
FROM
(
    SELECT CAST(unhex('01580180808080108A80808010'), 'AggregateFunction(groupConcat(\',\', 10), String)') AS state
); -- { serverError BAD_ARGUMENTS }

-- Check for non-monotonic offsets
SELECT hex(groupConcatMerge(',', 10)(state))
FROM
(
    SELECT CAST(unhex('0141010100'), 'AggregateFunction(groupConcat(\',\', 10), String)') AS state
); -- { serverError BAD_ARGUMENTS }
