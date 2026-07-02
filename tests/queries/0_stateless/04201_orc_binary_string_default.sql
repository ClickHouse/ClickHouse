-- Tags: no-fasttest
-- Test: exercises ORC writer with `output_format_orc_string_as_string=0` triggering BINARY schema path.
-- Covers: src/Processors/Formats/Impl/ORCBlockOutputFormat.cpp:152 — `return orc::createPrimitiveType(orc::TypeKind::BINARY)`
-- After this PR's default change to `output_format_orc_string_as_string=1`, no test exercises the BINARY path.

SET engine_file_truncate_on_insert = 1;

-- Write same String data with both settings.
INSERT INTO FUNCTION file(currentDatabase() || '_04201_binary.orc', 'ORC', 's String')
    SELECT 'hello' AS s FROM numbers(3)
    SETTINGS output_format_orc_string_as_string = 0;

INSERT INTO FUNCTION file(currentDatabase() || '_04201_string.orc', 'ORC', 's String')
    SELECT 'hello' AS s FROM numbers(3)
    SETTINGS output_format_orc_string_as_string = 1;

-- Schema MUST differ between BINARY and STRING types — proves the BINARY branch was actually taken.
SELECT
    length((SELECT * FROM file(currentDatabase() || '_04201_binary.orc', 'RawBLOB')))
        != length((SELECT * FROM file(currentDatabase() || '_04201_string.orc', 'RawBLOB'))) AS schemas_differ;

-- Round-trip: BINARY-typed ORC must read back correctly.
SELECT s FROM file(currentDatabase() || '_04201_binary.orc', 'ORC') ORDER BY s;
SELECT s FROM file(currentDatabase() || '_04201_string.orc', 'ORC') ORDER BY s;
