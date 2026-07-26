DROP TABLE IF EXISTS tab_coarse_ddl;

-- The argument is gated by an experimental MergeTree setting.
CREATE TABLE tab_coarse_ddl (id UInt64, s String, INDEX idx s TYPE text(tokenizer = splitByNonAlpha, coarse_granularity = 8192))
ENGINE = MergeTree ORDER BY id; -- { serverError SUPPORT_IS_DISABLED }

-- The argument must be a non-negative integer.
CREATE TABLE tab_coarse_ddl (id UInt64, s String, INDEX idx s TYPE text(tokenizer = splitByNonAlpha, coarse_granularity = -1))
ENGINE = MergeTree ORDER BY id
SETTINGS allow_experimental_text_index_coarse_granularity = 1; -- { serverError BAD_ARGUMENTS }

CREATE TABLE tab_coarse_ddl (id UInt64, s String, INDEX idx s TYPE text(tokenizer = splitByNonAlpha, coarse_granularity = 1.5))
ENGINE = MergeTree ORDER BY id
SETTINGS allow_experimental_text_index_coarse_granularity = 1; -- { serverError BAD_ARGUMENTS }

-- A bucket of one row is an exact posting list, so the value 1 is rejected.
CREATE TABLE tab_coarse_ddl (id UInt64, s String, INDEX idx s TYPE text(tokenizer = splitByNonAlpha, coarse_granularity = 1))
ENGINE = MergeTree ORDER BY id
SETTINGS allow_experimental_text_index_coarse_granularity = 1; -- { serverError BAD_ARGUMENTS }

-- Coarse posting lists cannot have positional data.
CREATE TABLE tab_coarse_ddl (id UInt64, s String, INDEX idx s TYPE text(tokenizer = splitByNonAlpha, coarse_granularity = 8192, support_phrase_search = 1))
ENGINE = MergeTree ORDER BY id
SETTINGS allow_experimental_text_index_coarse_granularity = 1, allow_experimental_text_index_phrase_search = 1; -- { serverError BAD_ARGUMENTS }

-- Zero (disabled) does not require the experimental setting and matches the index without the argument.
DROP TABLE IF EXISTS tab_coarse_zero;
DROP TABLE IF EXISTS tab_coarse_none;

CREATE TABLE tab_coarse_zero (id UInt64, s String, INDEX idx s TYPE text(tokenizer = splitByNonAlpha, coarse_granularity = 0))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 256;

CREATE TABLE tab_coarse_none (id UInt64, s String, INDEX idx s TYPE text(tokenizer = splitByNonAlpha))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 256;

INSERT INTO tab_coarse_zero SELECT number, concat('common w', toString(number % 3)) FROM numbers(8192);
INSERT INTO tab_coarse_none SELECT number, concat('common w', toString(number % 3)) FROM numbers(8192);

SELECT 'disabled index is identical',
    (SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'tab_coarse_zero' AND active)
  = (SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'tab_coarse_none' AND active);

DROP TABLE tab_coarse_zero;
DROP TABLE tab_coarse_none;

-- A valid definition survives detach and attach.
CREATE TABLE tab_coarse_ddl (id UInt64, s String, INDEX idx s TYPE text(tokenizer = splitByNonAlpha, coarse_granularity = 512))
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 256, allow_experimental_text_index_coarse_granularity = 1;

INSERT INTO tab_coarse_ddl SELECT number, concat('common w', toString(number % 3)) FROM numbers(8192);

DETACH TABLE tab_coarse_ddl;
ATTACH TABLE tab_coarse_ddl;

SELECT 'after attach', count() FROM tab_coarse_ddl WHERE hasToken(s, 'w1');

DROP TABLE tab_coarse_ddl;
