SET output_format_write_statistics = 0;

-- A real outer LIMIT reports rows before that outer limit, i.e. rows produced by the inner range.
SELECT * FROM (SELECT number FROM numbers(5) ORDER BY number LIMIT AFTER number >= 1) LIMIT 2 FORMAT JSONCompact SETTINGS exact_rows_before_limit = 1;

-- The `limit` setting is applied as an outer LIMIT wrapped around the query, so it reports the rows the
-- range produced, exactly like the explicit outer LIMIT above.
SELECT number FROM numbers(5) ORDER BY number LIMIT AFTER number >= 1 FORMAT JSONCompact SETTINGS limit = 2, exact_rows_before_limit = 1;
