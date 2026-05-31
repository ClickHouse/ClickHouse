-- Formats expose embedded documentation via system.formats.

-- Representative formats must have a non-empty description.
SELECT name, length(description) > 0 AS has_description
FROM system.formats
WHERE name IN ('JSONEachRow', 'CSV', 'Parquet', 'Native', 'Pretty')
ORDER BY name;

-- The CSV documentation is migrated in full, so it mentions the format.
SELECT position(description, 'CSV') > 0
FROM system.formats
WHERE name = 'CSV';

-- Alias formats reference their canonical format.
SELECT name, related
FROM system.formats
WHERE name IN ('TSV', 'NDJSON')
ORDER BY name;
