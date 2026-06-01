-- Formats expose embedded documentation via system.formats.

-- Representative formats must have a non-empty description.
-- Only core formats that are present in every build are checked here: formats backed by
-- optional libraries (Parquet, Arrow, Avro, ...) are absent from builds with ENABLE_LIBRARIES=0
-- (such as the fast-test build), and documentation is attached only to registered formats.
SELECT name, length(description) > 0 AS has_description
FROM system.formats
WHERE name IN ('JSONEachRow', 'CSV', 'TabSeparated', 'Native', 'Pretty')
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

-- Registry invariant (regression guard): attaching documentation must never invent a format
-- that the current build cannot use. A row carrying a description but usable for neither input
-- nor output would mean documentation registration created a phantom format entry.
SELECT count() FROM system.formats WHERE length(description) > 0 AND NOT is_input AND NOT is_output;
