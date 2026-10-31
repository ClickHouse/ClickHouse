SET allow_experimental_projection_text_index = 1;
SET enable_full_text_index = 1;

-- Tests that `dictionary_block_size` passed via the text index AST propagates into the
-- projection part's `index_granularity` at DDL time. Previously `getDefaultSettings()`
-- ran BEFORE `fillProjectionDescription` populated the index, so the default 512 was
-- always written regardless of the user's `dictionary_block_size = N` argument.
--
-- We assert it indirectly via `system.projection_parts.marks`: with N rows and an
-- index_granularity of G, marks = ceil(N / G). For 1000 rows:
--   - dictionary_block_size = 64  → marks ≈ ceil(1000/64) = 16
--   - dictionary_block_size = 512 → marks ≈ ceil(1000/512) = 2
-- Both cases include an extra implicit final mark so the exact count is the
-- next integer above the ceiling. We assert a range to keep the test resilient
-- to small layout changes while still distinguishing the two configurations.

DROP TABLE IF EXISTS t_block_size;

CREATE TABLE t_block_size
(
    id UInt32,
    msg String,
    PROJECTION idx INDEX msg TYPE text(tokenizer = 'splitByNonAlpha', dictionary_block_size = 64)
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO t_block_size SELECT number, concat('msg', toString(number)) FROM numbers(1000);

-- marks must be in the dictionary_block_size=64 range, i.e. roughly 1000/64 ≈ 16.
-- Previously (bug) marks would have been ≈ 2 from the default 512.
SELECT marks > 10 AND marks < 25
FROM system.projection_parts
WHERE database = currentDatabase() AND table = 't_block_size' AND active;

DROP TABLE t_block_size;

-- Sanity check: default value still applies when the argument is omitted.

DROP TABLE IF EXISTS t_default_block_size;

CREATE TABLE t_default_block_size
(
    id UInt32,
    msg String,
    PROJECTION idx INDEX msg TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO t_default_block_size SELECT number, concat('msg', toString(number)) FROM numbers(1000);

-- marks must be in the dictionary_block_size=512 (default) range, i.e. ≈ 1000/512 ≈ 2.
SELECT marks >= 2 AND marks <= 5
FROM system.projection_parts
WHERE database = currentDatabase() AND table = 't_default_block_size' AND active;

DROP TABLE t_default_block_size;
