-- Tags: no-parallel-replicas

-- Position lists larger than one segment (16K entries) must produce identical phrase results
-- through the streaming search, across both codecs and across the insert and merge paths.

SET enable_analyzer = 1;
SET enable_full_text_index = 1;
-- Common phrases would otherwise take the row-scan fallback and bypass the positions path entirely.
SET text_index_hint_max_selectivity = 1;

CREATE TABLE tab_none (
    id UInt32,
    message String,
    INDEX idx(message) TYPE text(tokenizer = splitByNonAlpha, support_phrase_search = 1, positions_codec = 'none')
)
ENGINE = MergeTree
ORDER BY id
SETTINGS allow_experimental_text_index_phrase_search = 1;

CREATE TABLE tab_pfor (
    id UInt32,
    message String,
    INDEX idx(message) TYPE text(tokenizer = splitByNonAlpha, support_phrase_search = 1, positions_codec = 'pfor')
)
ENGINE = MergeTree
ORDER BY id
SETTINGS allow_experimental_text_index_phrase_search = 1;

INSERT INTO tab_none(id, message) VALUES
    (1, 'abc def foo'),
    (2, 'abc def bar'),
    (3, 'abc baz foo'),
    (4, 'zzz foo bar'),
    (5, 'foo bar baz qux');

-- 17K docs per token exceed SEGMENT_TARGET_ENTRIES (16K), so every list spans multiple segments.
INSERT INTO tab_none(id, message) SELECT number + 10, 'hello clickhouse world' FROM numbers(17000);
INSERT INTO tab_none(id, message) SELECT number + 17010, 'hello world clickhouse' FROM numbers(17000);
-- A rare needle between the frequent runs: the streaming search must skip segments and still find it.
INSERT INTO tab_none(id, message) VALUES (34010, 'rare hello clickhouse marker');
INSERT INTO tab_none(id, message) SELECT number + 34020, 'hello clickhouse world' FROM numbers(17000);

INSERT INTO tab_pfor SELECT * FROM tab_none;

SELECT 'phrase results before merge (none, pfor)';

SELECT count() FROM tab_none WHERE hasPhrase(message, 'qux');
SELECT count() FROM tab_pfor WHERE hasPhrase(message, 'qux');
SELECT count() FROM tab_none WHERE hasPhrase(message, 'hello clickhouse world');
SELECT count() FROM tab_pfor WHERE hasPhrase(message, 'hello clickhouse world');
SELECT count() FROM tab_none WHERE hasPhrase(message, 'hello world clickhouse');
SELECT count() FROM tab_pfor WHERE hasPhrase(message, 'hello world clickhouse');
SELECT count() FROM tab_none WHERE hasPhrase(message, 'world hello');
SELECT count() FROM tab_pfor WHERE hasPhrase(message, 'world hello');

SELECT groupArray(id) FROM tab_none WHERE hasPhrase(message, 'abc def');
SELECT groupArray(id) FROM tab_pfor WHERE hasPhrase(message, 'abc def');
SELECT groupArray(id) FROM tab_none WHERE hasPhrase(message, 'rare hello');
SELECT groupArray(id) FROM tab_pfor WHERE hasPhrase(message, 'rare hello');
SELECT groupArray(id) FROM tab_none WHERE hasPhrase(message, 'hello clickhouse marker');
SELECT groupArray(id) FROM tab_pfor WHERE hasPhrase(message, 'hello clickhouse marker');
SELECT groupArray(id) FROM tab_none WHERE hasPhrase(message, 'foo bar');
SELECT groupArray(id) FROM tab_pfor WHERE hasPhrase(message, 'foo bar');

OPTIMIZE TABLE tab_none FINAL;
OPTIMIZE TABLE tab_pfor FINAL;

SELECT 'phrase results after merge (none, pfor)';

SELECT count() FROM tab_none WHERE hasPhrase(message, 'qux');
SELECT count() FROM tab_pfor WHERE hasPhrase(message, 'qux');
SELECT count() FROM tab_none WHERE hasPhrase(message, 'hello clickhouse world');
SELECT count() FROM tab_pfor WHERE hasPhrase(message, 'hello clickhouse world');
SELECT count() FROM tab_none WHERE hasPhrase(message, 'hello world clickhouse');
SELECT count() FROM tab_pfor WHERE hasPhrase(message, 'hello world clickhouse');
SELECT count() FROM tab_none WHERE hasPhrase(message, 'world hello');
SELECT count() FROM tab_pfor WHERE hasPhrase(message, 'world hello');

SELECT groupArray(id) FROM tab_none WHERE hasPhrase(message, 'abc def');
SELECT groupArray(id) FROM tab_pfor WHERE hasPhrase(message, 'abc def');
SELECT groupArray(id) FROM tab_none WHERE hasPhrase(message, 'rare hello');
SELECT groupArray(id) FROM tab_pfor WHERE hasPhrase(message, 'rare hello');
SELECT groupArray(id) FROM tab_none WHERE hasPhrase(message, 'hello clickhouse marker');
SELECT groupArray(id) FROM tab_pfor WHERE hasPhrase(message, 'hello clickhouse marker');
SELECT groupArray(id) FROM tab_none WHERE hasPhrase(message, 'foo bar');
SELECT groupArray(id) FROM tab_pfor WHERE hasPhrase(message, 'foo bar');

SELECT 'cross-check with brute force';

SELECT count() FROM tab_none WHERE hasPhrase(message, 'hello clickhouse');
SELECT count() FROM tab_none WHERE hasPhrase(message, 'hello clickhouse') SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab_pfor WHERE hasPhrase(message, 'hello clickhouse');
SELECT count() FROM tab_pfor WHERE hasPhrase(message, 'hello clickhouse') SETTINGS use_skip_indexes = 0;

DROP TABLE tab_pfor;
DROP TABLE tab_none;
