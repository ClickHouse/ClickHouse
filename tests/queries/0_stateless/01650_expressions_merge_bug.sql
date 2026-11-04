
SELECT
    NULL IN 
    (
        SELECT
            9223372036854775807,
            9223372036854775807
    ),
    NULL
FROM
(
    SELECT DISTINCT
        NULL,
        NULL,
        NULL IN 
        (
            SELECT (NULL, '-1')
        ),
        NULL
    FROM numbers(1024)
)
