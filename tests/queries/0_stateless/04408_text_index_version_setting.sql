-- Tags: no-parallel-replicas

-- Tests the `text_index_version` MergeTree setting that selects the on-disk text index format
-- version ('initial' or 'with_codec'), used to preserve forward compatibility during upgrades.

SET enable_analyzer = 1;

SELECT '-- default value';
SELECT value FROM system.merge_tree_settings WHERE name = 'text_index_version';

SELECT '-- invalid value is rejected';
DROP TABLE IF EXISTS tab_bad;
CREATE TABLE tab_bad
(
    id UInt32,
    str String,
    INDEX text_idx str TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree() ORDER BY id
SETTINGS text_index_version = 'nonsense'; -- { serverError BAD_ARGUMENTS }

SELECT '-- initial version: round-trip read';
DROP TABLE IF EXISTS tab_initial;
CREATE TABLE tab_initial
(
    id UInt32,
    str String,
    INDEX text_idx str TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree() ORDER BY id
-- 'initial' requires the 'none' codec; pin it so randomized settings cannot inject a codec.
SETTINGS index_granularity = 1, text_index_version = 'initial', text_index_posting_list_codec = 'none';

INSERT INTO tab_initial SELECT number, 'foo bar' FROM numbers(512);
INSERT INTO tab_initial SELECT number, 'foo baz' FROM numbers(512);

SELECT count() FROM tab_initial WHERE hasToken(str, 'foo');
SELECT count() FROM tab_initial WHERE hasToken(str, 'bar');
SELECT count() FROM tab_initial WHERE hasToken(str, 'qux');

SELECT '-- initial version: merge keeps the format readable';
OPTIMIZE TABLE tab_initial FINAL;
SELECT count() FROM tab_initial WHERE hasToken(str, 'foo');
SELECT count() FROM tab_initial WHERE hasToken(str, 'baz');

SELECT '-- with_codec version: round-trip read';
DROP TABLE IF EXISTS tab_with_codec;
CREATE TABLE tab_with_codec
(
    id UInt32,
    str String,
    INDEX text_idx str TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree() ORDER BY id
SETTINGS index_granularity = 1, text_index_version = 'with_codec', text_index_posting_list_codec = 'bitpacking';

INSERT INTO tab_with_codec SELECT number, 'foo bar' FROM numbers(512);
SELECT count() FROM tab_with_codec WHERE hasToken(str, 'foo');
SELECT count() FROM tab_with_codec WHERE hasToken(str, 'bar');

SELECT '-- initial version is incompatible with a posting list codec';
DROP TABLE IF EXISTS tab_conflict;
-- The conflict is rejected up front while validating the table settings on CREATE.
CREATE TABLE tab_conflict
(
    id UInt32,
    str String,
    INDEX text_idx str TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree() ORDER BY id
SETTINGS index_granularity = 1, text_index_version = 'initial', text_index_posting_list_codec = 'bitpacking'; -- { serverError BAD_ARGUMENTS }

SELECT '-- altering into the incompatible combination is also rejected';
DROP TABLE IF EXISTS tab_alter;
CREATE TABLE tab_alter
(
    id UInt32,
    str String,
    INDEX text_idx str TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree() ORDER BY id
SETTINGS index_granularity = 1, text_index_posting_list_codec = 'bitpacking';
ALTER TABLE tab_alter MODIFY SETTING text_index_version = 'initial'; -- { serverError BAD_ARGUMENTS }

DROP TABLE tab_initial;
DROP TABLE tab_with_codec;
DROP TABLE tab_alter;
