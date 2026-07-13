-- Tags: no-parallel-replicas

-- Test for bug 95944: text index preprocessor fails with ALIAS columns

SET use_skip_indexes = 1;
SET query_plan_direct_read_from_text_index = 1;

DROP TABLE IF EXISTS tab;

SELECT 'Test ALIAS column without preprocessor';

CREATE TABLE tab
(
    str Nullable(String),
    alias String ALIAS ifNull(str, 'default_name'),
    INDEX idx(alias) TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO tab VALUES ('Hello'), ('WORLD'), (NULL);

SELECT count() FROM tab WHERE hasToken(alias, 'Hello');
SELECT count() FROM tab WHERE hasToken(alias, 'WORLD');
SELECT count() FROM tab WHERE hasToken(alias, 'default');
SELECT count() FROM tab WHERE hasToken(alias, 'nonexistent');

SELECT '-- Verify original-case tokens are indexed (no preprocessor)';
SELECT token FROM mergeTreeTextIndex(currentDatabase(), tab, idx) ORDER BY token;

SELECT '-- Verify text index is used by hasToken';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab WHERE hasToken(alias, 'Hello')
) WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 1, 3;

DROP TABLE tab;

SELECT 'Test ALIAS column with preprocessor';

CREATE TABLE tab
(
    str Nullable(String),
    alias String ALIAS ifNull(str, 'DEFAULT_NAME'),
    INDEX idx(alias) TYPE text(
        tokenizer = 'splitByNonAlpha',
        preprocessor = lower(alias)
    )
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO tab VALUES ('Hello'), ('WORLD'), (NULL);

SELECT count() FROM tab WHERE hasToken(alias, 'hello');
SELECT count() FROM tab WHERE hasToken(alias, 'world');
SELECT count() FROM tab WHERE hasToken(alias, 'default');
SELECT count() FROM tab WHERE hasToken(alias, 'nonexistent');

SELECT '-- Verify preprocessed tokens are indexed (all lowercased)';
SELECT token FROM mergeTreeTextIndex(currentDatabase(), tab, idx) ORDER BY token;

SELECT '-- Verify text index is used by hasToken';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab WHERE hasToken(alias, 'hello')
) WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 1, 3;

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
    str String,
    arr String ALIAS str,
    INDEX idx(str) TYPE text(tokenizer = 'array')
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO tab VALUES ('hello'), ('world');

SELECT count() FROM tab WHERE hasToken(str, 'hello');
SELECT count() FROM tab WHERE hasToken(str, 'world');
SELECT count() FROM tab WHERE hasToken(str, 'nonexistent');

DROP TABLE tab;
