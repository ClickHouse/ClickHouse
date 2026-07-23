SELECT number
FROM numbers(10)
ORDER BY
    number ASC WITH FILL STEP 1,
    'aaa' ASC
LIMIT 1 BY number;

WITH 1 AS one
SELECT
    number AS num,
    one
FROM numbers(4)
ORDER BY
    num ASC WITH FILL STEP 1,
    one ASC
INTERPOLATE ( one AS 42 );
