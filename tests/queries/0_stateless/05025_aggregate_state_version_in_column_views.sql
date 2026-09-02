-- A sort (or any other operation that creates a view of a state column - a filter, a permutation)
-- must keep the state version of the column: the version affects how the states are serialized,
-- and `groupArray` over a state column serializes them into its own state and deserializes them back.
-- The view used to drop the version, so the states were serialized at version 0
-- and deserialized at the version of the fresh state type.

SELECT length(arraySlice(groupArray(x), 1, 1))
FROM (SELECT uniqState(number) AS x FROM numbers(10) GROUP BY number ORDER BY number);

SELECT uniqMerge(y) FROM
(
    SELECT arrayJoin(groupArray(x)) AS y
    FROM (SELECT uniqState(number) AS x FROM numbers(10) GROUP BY number ORDER BY number)
);

-- The same through a filter instead of a sort.
SELECT uniqMerge(y) FROM
(
    SELECT arrayJoin(groupArray(x)) AS y
    FROM (SELECT number AS n, uniqState(number) AS x FROM numbers(10) GROUP BY number) WHERE n != 3
);

-- The same for `quantileDeterministic`, whose state is versioned as well.
SELECT medianDeterministicMerge(y) FROM
(
    SELECT arrayJoin(groupArray(x)) AS y
    FROM (SELECT medianDeterministicState(number, number) AS x FROM numbers(10) GROUP BY number ORDER BY number)
);
