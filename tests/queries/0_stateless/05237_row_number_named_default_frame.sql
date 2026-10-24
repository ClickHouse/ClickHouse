-- Named windows must use the streaming default frame for `row_number`.
SELECT sum(row_num)
FROM
(
    SELECT row_number() OVER w AS row_num
    FROM numbers(10000000)
    WINDOW w AS ()
)
SETTINGS max_memory_usage = '64Mi';

-- Reusing the same named window must not restore partition buffering.
SELECT sum(first_row_num), sum(second_row_num)
FROM
(
    SELECT row_number() OVER w AS first_row_num, row_number() OVER w AS second_row_num
    FROM numbers(10000000)
    WINDOW w AS ()
)
SETTINGS max_memory_usage = '64Mi';

-- An aggregate sharing the named window retains its own default frame.
SELECT row_number() OVER w, sum(number) OVER w
FROM numbers(5)
WINDOW w AS ();

-- An explicit frame is preserved for every function using the named window.
SELECT row_number() OVER w, sum(number) OVER w
FROM numbers(5)
WINDOW w AS (ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING);
