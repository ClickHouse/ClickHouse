SET enable_analyzer=1;

SELECT * FROM view(
    SELECT row_number() OVER w
    FROM numbers(3)
    WINDOW w AS ()
);

SELECT * FROM viewExplain('EXPLAIN', '', (SELECT 1 WINDOW w0 AS ()));

-- Fuzzed, fails, but shouldn't crash server
SELECT
    number
FROM numbers(assumeNotNull(viewExplain('EXPLAIN', '', (
    SELECT 1 WINDOW w0 AS () QUALIFY number
)))) -- { serverError UNKNOWN_IDENTIFIER }
