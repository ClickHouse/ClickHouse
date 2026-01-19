SELECT number
FROM numbers(10)
ORDER BY
    number ASC WITH FILL STEP 1,
    'aaa' ASC
LIMIT 1 BY number;
