-- Tags: no-random-settings
-- Test formatRow('GeoJSON', ...) reusing one formatter: resetFormatter() runs after every row,
-- exercising GeoJSONRowOutputFormat::resetFormatterImpl (buffer-pointer re-sync after reset).

SET enable_analyzer = 1;

SELECT formatRow('GeoJSON', (n + 0.0, 2.0)::Point AS geometry, concat('s', toString(n)) AS name) AS r
FROM (SELECT number AS n FROM numbers(3)) ORDER BY n;

-- Same path with UTF-8 validation on, so the wrapper write buffer is recreated on each reset and the
-- re-fetched ostr must point at the new buffer for rows after the first.
SELECT formatRow('GeoJSON', (n + 0.0, 2.0)::Point AS geometry, concat('s', toString(n)) AS name) AS r
FROM (SELECT number AS n FROM numbers(3)) ORDER BY n
SETTINGS output_format_json_validate_utf8 = 1;
