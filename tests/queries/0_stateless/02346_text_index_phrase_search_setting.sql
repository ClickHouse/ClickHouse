-- `enable_text_index_phrase_search` allows `support_phrase_search`, and `allow_experimental_text_index_phrase_search` is its alias.

DROP TABLE IF EXISTS tab;

SELECT 'rejected without the setting';

CREATE TABLE tab (
	s String,
	INDEX idx(s) TYPE text(tokenizer = splitByNonAlpha, support_phrase_search = 1)
)
ENGINE = MergeTree
ORDER BY tuple(); -- { serverError SUPPORT_IS_DISABLED }

SELECT 'enable_text_index_phrase_search';

CREATE TABLE tab (
	s String,
	INDEX idx(s) TYPE text(tokenizer = splitByNonAlpha, support_phrase_search = 1)
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS enable_text_index_phrase_search = 1;

INSERT INTO tab VALUES ('the quick brown fox'), ('brown quick');

SELECT count() FROM tab WHERE hasPhrase(s, 'quick brown');

DROP TABLE tab;

SELECT 'the alias allow_experimental_text_index_phrase_search';

CREATE TABLE tab (
	s String,
	INDEX idx(s) TYPE text(tokenizer = splitByNonAlpha, support_phrase_search = 1)
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS allow_experimental_text_index_phrase_search = 1;

DROP TABLE tab;

SELECT 'tier';
SELECT name, tier FROM system.merge_tree_settings WHERE name = 'enable_text_index_phrase_search';
