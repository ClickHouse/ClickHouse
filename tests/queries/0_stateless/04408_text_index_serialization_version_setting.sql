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
-- 'v0_initial' requires the 'none' codec; pin it so randomized settings cannot inject a codec.
SETTINGS index_granularity = 1, text_index_serialization_version = 'v0_initial', text_index_posting_list_codec = 'none';

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
SETTINGS index_granularity = 1, text_index_serialization_version = 'v1_with_codec', text_index_posting_list_codec = 'bitpacking';

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
SETTINGS index_granularity = 1, text_index_serialization_version = 'v2_with_positions', allow_experimental_text_index_phrase_search = 1;

INSERT INTO tab_v2_with_positions SELECT number, 'foo bar baz' FROM numbers(512);
SELECT count() FROM tab_v2_with_positions WHERE hasPhrase(str, 'foo bar');
SELECT count() FROM tab_v2_with_positions WHERE hasPhrase(str, 'bar baz');
SELECT count() FROM tab_v2_with_positions WHERE hasPhrase(str, 'baz bar');

SELECT '-- v2_with_positions version: merge keeps the format readable';
INSERT INTO tab_v2_with_positions SELECT number, 'foo baz bar' FROM numbers(512);
OPTIMIZE TABLE tab_v2_with_positions FINAL;
SELECT count() FROM tab_v2_with_positions WHERE hasPhrase(str, 'foo bar');
SELECT count() FROM tab_v2_with_positions WHERE hasPhrase(str, 'baz bar');

SELECT '-- v0_initial version is incompatible with a posting list codec';
DROP TABLE IF EXISTS tab_conflict;
-- The conflict is rejected up front while validating the table settings on CREATE.
CREATE TABLE tab_conflict
(
    id UInt32,
    str String,
    INDEX text_idx str TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree() ORDER BY id
SETTINGS index_granularity = 1, text_index_serialization_version = 'v0_initial', text_index_posting_list_codec = 'bitpacking'; -- { serverError BAD_ARGUMENTS }

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
ALTER TABLE tab_alter MODIFY SETTING text_index_serialization_version = 'v0_initial'; -- { serverError BAD_ARGUMENTS }

SELECT '-- a posting list codec index argument requires at least the v1_with_codec version';
-- The table-level codec setting is pinned to 'none', so the conflict comes from the index argument alone.
DROP TABLE IF EXISTS tab_codec_arg;
CREATE TABLE tab_codec_arg
(
    id UInt32,
    str String,
    INDEX text_idx str TYPE text(tokenizer = 'splitByNonAlpha', posting_list_codec = 'bitpacking')
)
ENGINE = MergeTree() ORDER BY id
SETTINGS text_index_serialization_version = 'v0_initial', text_index_posting_list_codec = 'none'; -- { serverError BAD_ARGUMENTS }

SELECT '-- phrase search index requires the v2_with_positions version on CREATE';
-- The explicit contradiction is rejected up front: positions cannot be represented
-- in the pinned format, so the combination is likely a user error.
DROP TABLE IF EXISTS tab_positions_pinned;
CREATE TABLE tab_positions_pinned
(
    id UInt32,
    str String,
    INDEX text_idx str TYPE text(tokenizer = 'splitByNonAlpha', support_phrase_search = 1)
)
ENGINE = MergeTree() ORDER BY id
SETTINGS text_index_serialization_version = 'v1_with_codec', allow_experimental_text_index_phrase_search = 1; -- { serverError BAD_ARGUMENTS }

SELECT '-- adding a phrase search index on a table pinned to an older version is also rejected';
DROP TABLE IF EXISTS tab_add_index;
CREATE TABLE tab_add_index
(
    id UInt32,
    str String
)
ENGINE = MergeTree() ORDER BY id
SETTINGS text_index_serialization_version = 'v1_with_codec', allow_experimental_text_index_phrase_search = 1;
ALTER TABLE tab_add_index ADD INDEX text_idx str TYPE text(tokenizer = 'splitByNonAlpha', support_phrase_search = 1); -- { serverError BAD_ARGUMENTS }

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
DROP TABLE tab_alter;
DROP TABLE tab_add_index;
