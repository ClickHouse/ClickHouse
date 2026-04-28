-- Bug 95944: text index preprocessor fails with ALIAS columns

DROP TABLE IF EXISTS tab;

SELECT 'Test ALIAS column without preprocessor';

CREATE TABLE tab
(
    provider Nullable(String),
    name String ALIAS ifNull(provider, 'default_name'),
    INDEX name_text_idx(name) TYPE text(tokenizer = splitByNonAlpha)
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO tab VALUES ('Hello'), ('WORLD'), (NULL);

SELECT count() FROM tab WHERE hasToken(name, 'Hello');
SELECT count() FROM tab WHERE hasToken(name, 'WORLD');
SELECT count() FROM tab WHERE hasToken(name, 'default');
SELECT count() FROM tab WHERE hasToken(name, 'nonexistent');

SELECT '-- Verify original-case tokens are indexed (no preprocessor)';
SELECT token FROM mergeTreeTextIndex(currentDatabase(), tab, name_text_idx) ORDER BY token;

SELECT '-- Verify text index is used by hasToken';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab WHERE hasToken(name, 'Hello')
) WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 3;

DROP TABLE tab;

SELECT 'Test ALIAS column with preprocessor';

CREATE TABLE tab
(
    provider Nullable(String),
    name String ALIAS ifNull(provider, 'default_name'),
    INDEX name_text_idx(name) TYPE text(
        tokenizer = splitByNonAlpha,
        preprocessor = lower(name)
    )
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO tab VALUES ('Hello'), ('WORLD'), (NULL);

SELECT count() FROM tab WHERE hasToken(name, 'hello');
SELECT count() FROM tab WHERE hasToken(name, 'world');
SELECT count() FROM tab WHERE hasToken(name, 'default');
SELECT count() FROM tab WHERE hasToken(name, 'nonexistent');

SELECT '-- Verify preprocessed tokens are indexed (all lowercased)';
SELECT token FROM mergeTreeTextIndex(currentDatabase(), tab, name_text_idx) ORDER BY token;

SELECT '-- Verify text index is used by hasToken';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab WHERE hasToken(name, 'hello')
) WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 3;

DROP TABLE tab;

SELECT 'Test ALIAS column with Array type and preprocessor';

CREATE TABLE tab
(
    tags Array(String),
    lower_tags Array(String) ALIAS arrayMap(x -> lower(x), tags),
    INDEX idx(lower_tags) TYPE text(
        tokenizer = array,
        preprocessor = concat(lower_tags, '_suffix')
    )
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO tab VALUES (['Foo', 'BAR']), (['Baz']);

SELECT count() FROM tab WHERE has(lower_tags, 'foo');
SELECT count() FROM tab WHERE has(lower_tags, 'bar');
SELECT count() FROM tab WHERE has(lower_tags, 'baz');
SELECT count() FROM tab WHERE has(lower_tags, 'nonexistent');

SELECT '-- Verify preprocessed array tokens are indexed';
SELECT token FROM mergeTreeTextIndex(currentDatabase(), tab, idx) ORDER BY token;

DROP TABLE tab;

SELECT 'Test ALIAS name colliding with tokenizer identifier';

CREATE TABLE tab
(
    s String,
    `array` String ALIAS s,
    INDEX idx(s) TYPE text(tokenizer = array)
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO tab VALUES ('hello'), ('world');

SELECT count() FROM tab WHERE has(s, 'hello');
SELECT count() FROM tab WHERE has(s, 'world');
SELECT count() FROM tab WHERE has(s, 'nonexistent');

DROP TABLE tab;
