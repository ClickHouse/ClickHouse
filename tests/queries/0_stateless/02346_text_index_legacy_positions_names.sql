-- Tags: no-fasttest

-- The old names `positions` (text index argument, now `support_phrase_search`) and
-- `allow_experimental_text_index_positions` (MergeTree setting, now `allow_experimental_text_index_phrase_search`)
-- are still accepted, so tables created with them load.

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt64,
    s String,
    INDEX idx (s) TYPE text(tokenizer = splitByNonAlpha, positions = 1)
)
ENGINE = MergeTree ORDER BY id
SETTINGS allow_experimental_text_index_positions = 1;

INSERT INTO tab VALUES (0, 'apple banana apple'), (1, 'banana cherry'), (2, 'cherry apple');

SELECT token, row_id, positions FROM mergeTreeTextIndex(currentDatabase(), tab, idx) ORDER BY token, row_id;

DETACH TABLE tab;
ATTACH TABLE tab;

SELECT token, row_id, positions FROM mergeTreeTextIndex(currentDatabase(), tab, idx) ORDER BY token, row_id;

ALTER TABLE tab ADD COLUMN x UInt8;
SELECT count() FROM tab;

DROP TABLE tab;

-- The old and the new argument name cannot be used together.
DROP TABLE IF EXISTS tab;
CREATE TABLE tab (id UInt64, s String, INDEX idx (s) TYPE text(tokenizer = splitByNonAlpha, positions = 1, support_phrase_search = 1))
ENGINE = MergeTree ORDER BY id
SETTINGS allow_experimental_text_index_phrase_search = 1; -- { serverError BAD_ARGUMENTS }

-- The old argument name is experimental like the new one.
DROP TABLE IF EXISTS tab;
CREATE TABLE tab (id UInt64, s String, INDEX idx (s) TYPE text(tokenizer = splitByNonAlpha, positions = 1))
ENGINE = MergeTree ORDER BY id; -- { serverError SUPPORT_IS_DISABLED }
