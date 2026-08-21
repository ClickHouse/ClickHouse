-- Tags: no-parallel-replicas

-- Tests the `text_index_serialization_version` MergeTree setting that selects the on-disk text index format
-- version ('v0_initial', 'v1_with_codec' or 'v2_with_positions'), used to preserve forward compatibility
-- during upgrades.

SET enable_analyzer = 1;

SELECT '-- default value';
SELECT value FROM system.merge_tree_settings WHERE name = 'text_index_serialization_version';

SELECT '-- invalid value is rejected';
DROP TABLE IF EXISTS tab_bad;
CREATE TABLE tab_bad
(
    id UInt32,
    str String,
    INDEX text_idx str TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree() ORDER BY id
SETTINGS text_index_serialization_version = 'nonsense'; -- { serverError BAD_ARGUMENTS }

SELECT '-- v0_initial version: round-trip read';
DROP TABLE IF EXISTS tab_v0_initial;
CREATE TABLE tab_v0_initial
(
    id UInt32,
    str String,
    INDEX text_idx str TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree() ORDER BY id
-- Pin the codec to 'none': a randomized non-'none' codec would silently bump the format to 'v1_with_codec'.
SETTINGS index_granularity = 64, text_index_serialization_version = 'v0_initial', text_index_posting_list_codec = 'none';

INSERT INTO tab_v0_initial SELECT number, 'foo bar' FROM numbers(512);
INSERT INTO tab_v0_initial SELECT number, 'foo baz' FROM numbers(512);

SELECT count() FROM tab_v0_initial WHERE hasToken(str, 'foo');
SELECT count() FROM tab_v0_initial WHERE hasToken(str, 'bar');
SELECT count() FROM tab_v0_initial WHERE hasToken(str, 'qux');

SELECT '-- v0_initial version: merge keeps the format readable';
OPTIMIZE TABLE tab_v0_initial FINAL;
SELECT count() FROM tab_v0_initial WHERE hasToken(str, 'foo');
SELECT count() FROM tab_v0_initial WHERE hasToken(str, 'baz');

SELECT '-- v1_with_codec version: round-trip read';
DROP TABLE IF EXISTS tab_v1_with_codec;
CREATE TABLE tab_v1_with_codec
(
    id UInt32,
    str String,
    INDEX text_idx str TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree() ORDER BY id
SETTINGS index_granularity = 64, text_index_serialization_version = 'v1_with_codec', text_index_posting_list_codec = 'bitpacking';

INSERT INTO tab_v1_with_codec SELECT number, 'foo bar' FROM numbers(512);
SELECT count() FROM tab_v1_with_codec WHERE hasToken(str, 'foo');
SELECT count() FROM tab_v1_with_codec WHERE hasToken(str, 'bar');

SELECT '-- v2_with_positions version: phrase search round-trip';
DROP TABLE IF EXISTS tab_v2_with_positions;
CREATE TABLE tab_v2_with_positions
(
    id UInt32,
    str String,
    INDEX text_idx str TYPE text(tokenizer = 'splitByNonAlpha', support_phrase_search = 1)
)
ENGINE = MergeTree() ORDER BY id
SETTINGS index_granularity = 64, text_index_serialization_version = 'v2_with_positions', allow_experimental_text_index_phrase_search = 1;

INSERT INTO tab_v2_with_positions SELECT number, 'foo bar baz' FROM numbers(512);
SELECT count() FROM tab_v2_with_positions WHERE hasPhrase(str, 'foo bar');
SELECT count() FROM tab_v2_with_positions WHERE hasPhrase(str, 'bar baz');
SELECT count() FROM tab_v2_with_positions WHERE hasPhrase(str, 'baz bar');

SELECT '-- v2_with_positions version: merge keeps the format readable';
INSERT INTO tab_v2_with_positions SELECT number, 'foo baz bar' FROM numbers(512);
OPTIMIZE TABLE tab_v2_with_positions FINAL;
SELECT count() FROM tab_v2_with_positions WHERE hasPhrase(str, 'foo bar');
SELECT count() FROM tab_v2_with_positions WHERE hasPhrase(str, 'baz bar');

SELECT '-- a posting list codec setting overrides the v0_initial preference';
-- The version setting is only a preference: the codec cannot be represented in 'v0_initial',
-- so the index is silently written in 'v1_with_codec' and stays readable.
DROP TABLE IF EXISTS tab_codec_override;
CREATE TABLE tab_codec_override
(
    id UInt32,
    str String,
    INDEX text_idx str TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree() ORDER BY id
SETTINGS index_granularity = 64, text_index_serialization_version = 'v0_initial', text_index_posting_list_codec = 'bitpacking';

INSERT INTO tab_codec_override SELECT number, 'foo bar' FROM numbers(512);
SELECT count() FROM tab_codec_override WHERE hasToken(str, 'foo');

SELECT '-- altering into the same combination also keeps the index writable';
DROP TABLE IF EXISTS tab_alter;
CREATE TABLE tab_alter
(
    id UInt32,
    str String,
    INDEX text_idx str TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree() ORDER BY id
SETTINGS index_granularity = 64, text_index_posting_list_codec = 'bitpacking';
ALTER TABLE tab_alter MODIFY SETTING text_index_serialization_version = 'v0_initial';
INSERT INTO tab_alter SELECT number, 'foo bar' FROM numbers(512);
SELECT count() FROM tab_alter WHERE hasToken(str, 'bar');

SELECT '-- a posting list codec index argument also overrides the v0_initial preference';
-- The table-level codec setting is pinned to 'none', so the bump to 'v1_with_codec' comes from the index argument alone.
DROP TABLE IF EXISTS tab_codec_arg;
CREATE TABLE tab_codec_arg
(
    id UInt32,
    str String,
    INDEX text_idx str TYPE text(tokenizer = 'splitByNonAlpha', posting_list_codec = 'bitpacking')
)
ENGINE = MergeTree() ORDER BY id
SETTINGS index_granularity = 64, text_index_serialization_version = 'v0_initial', text_index_posting_list_codec = 'none';

INSERT INTO tab_codec_arg SELECT number, 'foo bar' FROM numbers(512);
SELECT count() FROM tab_codec_arg WHERE hasToken(str, 'foo');

SELECT '-- a phrase search index overrides an older version preference on CREATE';
-- Positions cannot be represented in 'v1_with_codec', so the index
-- is silently written in 'v2_with_positions' and phrase search works.
DROP TABLE IF EXISTS tab_positions_pinned;
CREATE TABLE tab_positions_pinned
(
    id UInt32,
    str String,
    INDEX text_idx str TYPE text(tokenizer = 'splitByNonAlpha', support_phrase_search = 1)
)
ENGINE = MergeTree() ORDER BY id
SETTINGS index_granularity = 64, text_index_serialization_version = 'v1_with_codec', allow_experimental_text_index_phrase_search = 1;

INSERT INTO tab_positions_pinned SELECT number, 'foo bar baz' FROM numbers(512);
SELECT count() FROM tab_positions_pinned WHERE hasPhrase(str, 'foo bar');
SELECT count() FROM tab_positions_pinned WHERE hasPhrase(str, 'baz bar');

SELECT '-- adding a phrase search index on a table pinned to an older version';
DROP TABLE IF EXISTS tab_add_index;
CREATE TABLE tab_add_index
(
    id UInt32,
    str String
)
ENGINE = MergeTree() ORDER BY id
SETTINGS index_granularity = 64, text_index_serialization_version = 'v1_with_codec', allow_experimental_text_index_phrase_search = 1;
ALTER TABLE tab_add_index ADD INDEX text_idx str TYPE text(tokenizer = 'splitByNonAlpha', support_phrase_search = 1);
INSERT INTO tab_add_index SELECT number, 'foo bar' FROM numbers(512);
SELECT count() FROM tab_add_index WHERE hasPhrase(str, 'foo bar');

SELECT '-- pinning the version on an existing phrase search table keeps the index writable';
-- On an existing table the setting is only a preference: the index keeps being written
-- in the 'v2_with_positions' format it requires, so inserts and merges never fail.
ALTER TABLE tab_v2_with_positions MODIFY SETTING text_index_serialization_version = 'v1_with_codec';
INSERT INTO tab_v2_with_positions SELECT number, 'foo bar qux' FROM numbers(512);
OPTIMIZE TABLE tab_v2_with_positions FINAL;
SELECT count() FROM tab_v2_with_positions WHERE hasPhrase(str, 'bar qux');

DROP TABLE tab_v0_initial;
DROP TABLE tab_v1_with_codec;
DROP TABLE tab_v2_with_positions;
DROP TABLE tab_codec_override;
DROP TABLE tab_alter;
DROP TABLE tab_codec_arg;
DROP TABLE tab_positions_pinned;
DROP TABLE tab_add_index;
