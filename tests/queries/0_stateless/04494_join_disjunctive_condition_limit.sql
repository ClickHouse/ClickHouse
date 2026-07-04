-- Positive test: 10 AND groups of 2 OR branches = 2^10 = 1024 conditions, exactly at the default limit
SELECT count()
FROM numbers(1) AS left_table
INNER JOIN numbers(1) AS right_table
ON (left_table.number = right_table.number OR left_table.number + 1 = right_table.number + 1)
    AND (left_table.number + 2 = right_table.number + 2 OR left_table.number + 3 = right_table.number + 3)
    AND (left_table.number + 4 = right_table.number + 4 OR left_table.number + 5 = right_table.number + 5)
    AND (left_table.number + 6 = right_table.number + 6 OR left_table.number + 7 = right_table.number + 7)
    AND (left_table.number + 8 = right_table.number + 8 OR left_table.number + 9 = right_table.number + 9)
    AND (left_table.number + 10 = right_table.number + 10 OR left_table.number + 11 = right_table.number + 11)
    AND (left_table.number + 12 = right_table.number + 12 OR left_table.number + 13 = right_table.number + 13)
    AND (left_table.number + 14 = right_table.number + 14 OR left_table.number + 15 = right_table.number + 15)
    AND (left_table.number + 16 = right_table.number + 16 OR left_table.number + 17 = right_table.number + 17)
    AND (left_table.number + 18 = right_table.number + 18 OR left_table.number + 19 = right_table.number + 19)
SETTINGS allow_general_join_planning = 1, join_algorithm = 'hash', enable_analyzer = 1, enable_parallel_replicas = 0;

-- Negative test: 11 AND groups of 2 OR branches = 2^11 = 2048 conditions, exceeds the default limit
SELECT count()
FROM numbers(1) AS left_table
INNER JOIN numbers(1) AS right_table
ON (left_table.number = right_table.number OR left_table.number + 1 = right_table.number + 1)
    AND (left_table.number + 2 = right_table.number + 2 OR left_table.number + 3 = right_table.number + 3)
    AND (left_table.number + 4 = right_table.number + 4 OR left_table.number + 5 = right_table.number + 5)
    AND (left_table.number + 6 = right_table.number + 6 OR left_table.number + 7 = right_table.number + 7)
    AND (left_table.number + 8 = right_table.number + 8 OR left_table.number + 9 = right_table.number + 9)
    AND (left_table.number + 10 = right_table.number + 10 OR left_table.number + 11 = right_table.number + 11)
    AND (left_table.number + 12 = right_table.number + 12 OR left_table.number + 13 = right_table.number + 13)
    AND (left_table.number + 14 = right_table.number + 14 OR left_table.number + 15 = right_table.number + 15)
    AND (left_table.number + 16 = right_table.number + 16 OR left_table.number + 17 = right_table.number + 17)
    AND (left_table.number + 18 = right_table.number + 18 OR left_table.number + 19 = right_table.number + 19)
    AND (left_table.number + 20 = right_table.number + 20 OR left_table.number + 21 = right_table.number + 21)
SETTINGS allow_general_join_planning = 1, join_algorithm = 'hash', enable_analyzer = 1, enable_parallel_replicas = 0; -- { serverError INVALID_JOIN_ON_EXPRESSION }
