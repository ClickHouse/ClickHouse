SELECT * FROM view(
    SELECT row_number() OVER w
    FROM numbers(3)
    WINDOW w AS ()
);

SELECT * FROM viewExplain('EXPLAIN', '', (SELECT 1 WINDOW w0 AS ()));
