-- Tags: no-parallel-replicas
-- no-parallel-replicas: the distributed plan forces `use_skip_indexes_on_data_read = 0`, which turns
-- the direct read off, so those runs would only ever exercise the fallback path.

-- Checks that a text index search returns the same rows while a lightweight update is pending on the
-- table as it does without, and as it does with direct read turned off.

SET query_plan_direct_read_from_text_index = 1;
SET enable_lightweight_update = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab (
    id UInt64,
    c UInt64,
    s String,
    INDEX idx s TYPE text(tokenizer = splitByNonAlpha))
ENGINE = MergeTree ORDER BY id
SETTINGS
    enable_block_number_column = 1,
    enable_block_offset_column = 1,
    apply_patches_on_merge = 0; -- keep the patch pending

INSERT INTO tab SELECT number, 0, concat('tok', toString(number % 10), ' word') FROM numbers(1000);

SYSTEM STOP MERGES tab;
UPDATE tab SET c = 1 WHERE id < 10;
-- the update is now pending

-- 100 rows match `tok1`, of which only `id = 1` was updated.
SELECT count(), sum(c) FROM tab WHERE hasToken(s, 'tok1') SETTINGS query_plan_direct_read_from_text_index = 1;
SELECT count(), sum(c) FROM tab WHERE hasToken(s, 'tok1') SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT count(), sum(c) FROM tab WHERE hasToken(s, 'tok1') SETTINGS use_skip_indexes = 0;

-- A second predicate alongside the search: the rows disappeared here too.
SELECT id, c FROM tab WHERE hasToken(s, 'tok1') AND id < 25 ORDER BY id;

SELECT '-- materialized';
SYSTEM START MERGES tab;
ALTER TABLE tab MODIFY SETTING apply_patches_on_merge = 1;
OPTIMIZE TABLE tab FINAL;

SELECT count(), sum(c) FROM tab WHERE hasToken(s, 'tok1');

-- Direct read must be used
SELECT count() > 0 FROM
(
    EXPLAIN actions = 1 SELECT count() FROM tab WHERE hasToken(s, 'tok1')
) WHERE explain LIKE '%\_\_text_index%';

DROP TABLE tab;

SELECT '-- With preprocessor';

CREATE TABLE tab
(
    id UInt64,
    c UInt64,
    s String,
    INDEX idx_s s TYPE text(tokenizer = splitByNonAlpha, preprocessor = lower(s))
)
ENGINE = MergeTree ORDER BY id
SETTINGS
    enable_block_number_column = 1,
    enable_block_offset_column = 1,
    apply_patches_on_merge = 0;

INSERT INTO tab SELECT number, 0, if(number < 10, 'Hello World', 'foo bar') FROM numbers(1000);

-- The preprocessor applies on the index path only, so these two legitimately differ.
SELECT count() FROM tab WHERE hasAnyTokens(s, 'hello') SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab WHERE hasAnyTokens(s, 'hello') SETTINGS use_skip_indexes = 1;

SYSTEM STOP MERGES tab;
UPDATE tab SET c = 1 WHERE id = 500;

-- Same two answers with an update pending on `c`, which the index does not cover.
SELECT count() FROM tab WHERE hasAnyTokens(s, 'hello') SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab WHERE hasAnyTokens(s, 'hello') SETTINGS use_skip_indexes = 1;

DROP TABLE tab;

-- Updating the indexed column itself. The search must see the new value.
SELECT 'patched indexed column';

CREATE TABLE tab (
    id UInt64,
    s String,
    INDEX idx s TYPE text(tokenizer = splitByNonAlpha))
ENGINE = MergeTree ORDER BY id
SETTINGS
    enable_block_number_column = 1,
    enable_block_offset_column = 1,
    apply_patches_on_merge = 0;

INSERT INTO tab SELECT number, concat('tok', toString(number % 10), ' word') FROM numbers(1000);

SYSTEM STOP MERGES tab;
UPDATE tab SET s = 'zebra word' WHERE id = 1;

SELECT count() FROM tab WHERE hasToken(s, 'tok1') SETTINGS query_plan_direct_read_from_text_index = 1;
SELECT count() FROM tab WHERE hasToken(s, 'tok1') SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT count() FROM tab WHERE hasToken(s, 'tok1') SETTINGS use_skip_indexes = 0;

SELECT count() FROM tab WHERE hasToken(s, 'zebra') SETTINGS query_plan_direct_read_from_text_index = 1;
SELECT count() FROM tab WHERE hasToken(s, 'zebra') SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT count() FROM tab WHERE hasToken(s, 'zebra') SETTINGS use_skip_indexes = 0;

DROP TABLE tab;
