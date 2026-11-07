SELECT madMerge(state)
FROM
(
    SELECT number % 4 AS key, madState(number) AS state
    FROM numbers(100)
    GROUP BY key
);

SELECT mad(number)
FROM numbers(100);

-- Empty nullable states must preserve the NaN default of plain mad.
SELECT isNaN(finalizeAggregation(state))
FROM
(
    SELECT madState(CAST(NULL AS Nullable(Int32))) AS state
    FROM numbers(4)
);

SELECT isNaN(madMerge(state))
FROM
(
    SELECT number % 2 AS key, madState(CAST(NULL AS Nullable(Int32))) AS state
    FROM numbers(4)
    GROUP BY key
);

-- An empty nullable state must not mask values from a non-empty state.
SELECT madMerge(state)
FROM
(
    SELECT number % 2 AS key,
           madState(if(number % 2 = 0, CAST(NULL AS Nullable(Int32)), toNullable(toInt32(number)))) AS state
    FROM numbers(8)
    GROUP BY key
);
