-- Check if we catch overflow on num_rows

-- data_size = 4, data = AAAA, num_rows = 2^63
SELECT hex(groupConcatMerge(',', 10)(state))
FROM
(
    SELECT CAST(unhex('044141414180808080808080808001'), 'AggregateFunction(groupConcat(\',\', 10), String)') AS state
) -- { serverError BAD_ARGUMENTS }
