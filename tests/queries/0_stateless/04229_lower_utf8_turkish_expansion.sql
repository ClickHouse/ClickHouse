-- Tags: no-fasttest
-- Test: exercises `lowerUTF8` `U_BUFFER_OVERFLOW_ERROR` retry path at LowerUpperUTF8Impl.h:122-128
-- Covers: Turkish İ (U+0130) expands 2→3 bytes during lowercase, triggering buffer overflow retry

-- Turkish İ (LATIN CAPITAL LETTER I WITH DOT ABOVE) lowercases to i + combining dot above
-- This is a 1.5x byte expansion (2 bytes → 3 bytes), which can trigger U_BUFFER_OVERFLOW_ERROR
-- when the initial buffer (sized to input) is insufficient for the expanded output.

-- Single row with enough İ to trigger overflow: 1000 İ chars = 2000 input bytes → 3000 output bytes
SELECT 
    'single_row_expansion' AS test_case,
    length(input) AS input_bytes,
    length(output) AS output_bytes,
    output_bytes > input_bytes AS expanded
FROM (
    SELECT repeat('İ', 1000) AS input, lowerUTF8(repeat('İ', 1000)) AS output
) ORDER BY test_case;

-- Multiple rows to exercise per-row retry path with expansion
SELECT 
    'multi_row_expansion' AS test_case,
    sum(length(s)) AS total_input_bytes,
    sum(length(lowerUTF8(s))) AS total_output_bytes,
    sum(length(lowerUTF8(s))) > sum(length(s)) AS expanded
FROM (
    SELECT arrayJoin([repeat('İ', 500), repeat('İ', 500), repeat('İ', 500)]) AS s
) ORDER BY test_case;

-- Mixed batch: some rows expand (İ), some don't (normal Unicode)
-- This tests the retry path interleaved with non-expanding rows
SELECT 
    'mixed_batch' AS test_case,
    sum(length(s)) AS total_input_bytes,
    sum(length(lowerUTF8(s))) AS total_output_bytes
FROM (
    SELECT arrayJoin([
        repeat('İ', 100),      -- expands 1.5x
        'MÜNCHEN',             -- normal (no expansion)
        repeat('İ', 100),      -- expands 1.5x
        'ПРИВЕТ'               -- normal (no expansion)
    ]) AS s
) ORDER BY test_case;
