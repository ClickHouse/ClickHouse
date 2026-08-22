SET enable_analyzer = 1;

With
    A AS (
        SELECT * FROM numbers(10) ORDER BY number ASC
    ),
    B AS (
        SELECT * FROM numbers(10) ORDER BY number ASC
    ),
    C AS (
        SELECT * FROM numbers(10) ORDER BY number DESC
    )
SELECT
    *
FROM A
PASTE JOIN B AS B
PASTE JOIN C AS C
WHERE A.number % 2 == 0
SETTINGS query_plan_filter_push_down = 1;

SELECT '---------------------';

With
    A AS (
        SELECT * FROM numbers(10) ORDER BY number ASC
    ),
    B AS (
        SELECT * FROM numbers(10) ORDER BY number ASC
    ),
    C AS (
        SELECT * FROM numbers(10) ORDER BY number DESC
    )   
SELECT
    *
FROM A
PASTE JOIN B AS B
PASTE JOIN C AS C
WHERE A.number % 2 == 0
SETTINGS query_plan_filter_push_down = 0;
