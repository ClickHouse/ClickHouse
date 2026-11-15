-- Tags: no-fasttest
-- Tag no-fasttest: depends on libstemmer_c

-- Tests the text index postprocessor mechanism using stem() as the example expression:
-- the index stores the postprocessed (stemmed) tokens and search predicates route their
-- needle through the same postprocessor before the index lookup.
-- Covers: String, Array(String), Nullable(String) and Map column types, index inspection,
-- and the predicates hasToken / hasAllTokens / hasAnyTokens / IN / equals / hasPhrase /
-- startsWith / endsWith / like / mapContains*.
-- The morphological behavior of stem() itself (which conjugations share a stem across
-- languages, where irregular forms diverge) is tested in 01890_stem, not here.

DROP TABLE IF EXISTS tab;

SELECT '-- stem postprocessor: the index stores stemmed tokens and the needle is stemmed before lookup.';

-- Postprocessor: lower() folds case, stem() maps every word form to its root, so morphologically
-- related forms collapse to a single indexed token and any conjugation in the needle finds them.
CREATE TABLE tab
(
    id UInt64,
    val String,
    INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', postprocessor = stem(lower(val), 'en'))
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO tab VALUES
    (1, 'running'),     -- stem 'run'
    (2, 'runs'),        -- stem 'run'
    (3, 'run'),         -- stem 'run'
    (4, 'studied'),     -- stem 'studi'
    (5, 'studying');    -- stem 'studi'

-- Any form of 'run', including a differently-cased one, finds all three run-stem rows.
SELECT count() FROM tab WHERE hasToken(val, 'running');   -- needle -> 'run'; 3
SELECT count() FROM tab WHERE hasToken(val, 'RUNS');      -- needle -> lower -> stem 'run'; 3
-- A 'study' form not present in the data still matches via the shared stem 'studi'.
SELECT count() FROM tab WHERE hasToken(val, 'study');     -- needle -> 'studi'; rows 4,5 = 2
-- A token whose stem matches nothing returns no rows.
SELECT count() FROM tab WHERE hasToken(val, 'walker');    -- 0
-- hasAllTokens needs all stems in one row (none here); hasAnyTokens needs any (all 5).
SELECT count() FROM tab WHERE hasAllTokens(val, 'running studying');       -- 0
SELECT count() FROM tab WHERE hasAnyTokens(val, ['running', 'studying']);  -- 5

DROP TABLE tab;

SELECT '-- Array(String) column: postprocessor stems each array element.';
-- Note: lower() cannot be wrapped around an Array(String) column in the postprocessor
-- expression (it only accepts scalar strings). Use pre-lowercased data and call
-- stem(val, language) directly.

CREATE TABLE tab
(
    id UInt64,
    val Array(String),
    INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', postprocessor = stem(val, 'en'))
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO tab VALUES
    (1, ['running', 'studying']),   -- stems: 'run', 'studi'
    (2, ['collects', 'books']),     -- stems: 'collect', 'book'
    (3, ['run', 'collect']);        -- stems: 'run', 'collect'

-- 'running' → 'run'; rows 1 and 3 = 2.
SELECT count() FROM tab WHERE hasAllTokens(val, 'running');
-- 'runs' → 'run'; same 2.
SELECT count() FROM tab WHERE hasAllTokens(val, 'runs');
-- 'collection' → 'collect'; rows 2 and 3 = 2.
SELECT count() FROM tab WHERE hasAllTokens(val, 'collection');
-- 'study' → 'studi'; row 1 = 1.
SELECT count() FROM tab WHERE hasAllTokens(val, 'study');
-- No match: 0.
SELECT count() FROM tab WHERE hasAllTokens(val, 'xyz');
-- Row 1 has both 'run' and 'studi': 1.
SELECT count() FROM tab WHERE hasAllTokens(val, ['running', 'studying']);
-- No row has all three stems ('run', 'studi', 'collect'): 0.
SELECT count() FROM tab WHERE hasAllTokens(val, ['running', 'studying', 'collecting']);
-- 'studi' in row 1, 'collect' in rows 2 and 3 → 3.
SELECT count() FROM tab WHERE hasAnyTokens(val, ['studying', 'collecting']);
-- No match: 0.
SELECT count() FROM tab WHERE hasAnyTokens(val, ['xyz', 'abc']);

DROP TABLE tab;

SELECT '-- Nullable(String) column: NULL values are skipped by the index.';

CREATE TABLE tab
(
    id UInt64,
    val Nullable(String),
    INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', postprocessor = stem(lower(val), 'en'))
)
ENGINE = MergeTree ORDER BY id SETTINGS allow_nullable_key = 1;

-- Three rows share stem 'run', two share stem 'studi', interleaved with NULLs.
INSERT INTO tab VALUES
    (1, 'running'),     -- stem 'run'
    (2, NULL),
    (3, 'runs'),        -- stem 'run'
    (4, 'run'),         -- stem 'run'
    (5, NULL),
    (6, 'studies'),     -- stem 'studi'
    (7, 'studied');     -- stem 'studi'

-- Any morphological form of 'run' reaches all three run-stem rows; NULL rows are skipped.
SELECT count() FROM tab WHERE hasToken(val, 'running');   -- 3
SELECT count() FROM tab WHERE hasToken(val, 'runs');      -- 3
SELECT count() FROM tab WHERE hasToken(val, 'run');       -- 3
-- Either form of 'study' reaches both studi-stem rows.
SELECT count() FROM tab WHERE hasToken(val, 'studying');  -- 2
SELECT count() FROM tab WHERE hasToken(val, 'studied');   -- 2
-- 'walker' has its own stem; no match.
SELECT count() FROM tab WHERE hasToken(val, 'walker');    -- 0
-- 'run' and 'studi' never share a row.
SELECT count() FROM tab WHERE hasAllTokens(val, 'running studied');         -- 0
-- 3 run-stem rows ∪ 2 studi-stem rows = 5.
SELECT count() FROM tab WHERE hasAnyTokens(val, ['running', 'studied']);    -- 5

DROP TABLE tab;

SELECT '-- Index inspection: the index stores only stemmed tokens, not original words.';
-- Using mergeTreeTextIndex to read the posting-list dictionary directly.
-- With a stem postprocessor, the stored tokens are the stemmed forms.

DROP TABLE IF EXISTS tab;
CREATE TABLE tab
(
    id UInt64,
    val String,
    INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', postprocessor = stem(lower(val), 'en'))
)
ENGINE = MergeTree ORDER BY id;
SYSTEM STOP MERGES tab;

-- Five original words; the postprocessor maps them to three distinct stems.
INSERT INTO tab VALUES
    (1, 'running'),     -- stored as 'run'
    (2, 'runs'),        -- stored as 'run'
    (3, 'run'),         -- stored as 'run'
    (4, 'studying'),    -- stored as 'studi'
    (5, 'collection');  -- stored as 'collect'

-- The dictionary must contain exactly the stemmed forms — not the original words.
SELECT token, cardinality
FROM mergeTreeTextIndex(currentDatabase(), tab, idx)
ORDER BY token;

SYSTEM START MERGES tab;
DROP TABLE tab;

SELECT '-- val IN (...) routes set elements through the stem postprocessor.';
-- Without postprocessor-aware set tokenization, the index would look up the raw needle
-- (e.g. 'running') and miss the stemmed token ('run'), pruning matching rows.

CREATE TABLE tab
(
    id UInt64,
    val String,
    INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', postprocessor = stem(lower(val), 'en'))
)
ENGINE = MergeTree ORDER BY id;

-- Three rows share stem 'run', two share stem 'studi'.
INSERT INTO tab VALUES
    (1, 'running'),    -- stem 'run'
    (2, 'runs'),       -- stem 'run'
    (3, 'run'),        -- stem 'run'
    (4, 'studies'),    -- stem 'studi'
    (5, 'studied');    -- stem 'studi'

-- IN with all three morphological forms — each is exact-matched at row level; the index
-- must keep the granule for each needle (stem 'run' is in the index).
SELECT count() FROM tab WHERE val IN ('running', 'runs', 'run');   -- 3
-- IN with both 'studi' forms.
SELECT count() FROM tab WHERE val IN ('studies', 'studied');       -- 2
-- Single literal — exact match returns 1 row.
SELECT count() FROM tab WHERE val IN ('runs');                     -- 1
-- Stem-equivalent needle without a literal match: granule kept (stem 'studi' present),
-- but no row has the literal 'studying'.
SELECT count() FROM tab WHERE val IN ('studying');                 -- 0
-- Different stem — granule pruned.
SELECT count() FROM tab WHERE val IN ('walking');                  -- 0

DROP TABLE tab;

SELECT '-- equals: needle is stemmed before index lookup so morphologically equivalent rows are not pruned.';
-- equals is HINT mode: the index keeps the granule when the needle's stem is present,
-- but row-level evaluation is literal exact match. Each form returns its own row.

CREATE TABLE tab
(
    id UInt64,
    val String,
    INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', postprocessor = stem(lower(val), 'en'))
)
ENGINE = MergeTree ORDER BY id;

-- Three rows share stem 'run'.
INSERT INTO tab VALUES
    (1, 'running'),    -- stem 'run'
    (2, 'runs'),       -- stem 'run'
    (3, 'run'),        -- stem 'run'
    (4, 'cat');

-- Each morphological form matches its literal row; the index keeps the granule via stem 'run'.
SELECT count() FROM tab WHERE val = 'running';   -- 1
SELECT count() FROM tab WHERE val = 'runs';      -- 1
SELECT count() FROM tab WHERE val = 'run';       -- 1
-- The OR of all three literals covers every row in the stem-'run' family.
SELECT count() FROM tab WHERE val = 'running' OR val = 'runs' OR val = 'run';   -- 3
-- Different stem — granule pruned.
SELECT count() FROM tab WHERE val = 'walking';   -- 0

DROP TABLE tab;

SELECT '-- hasPhrase: phrase tokens are stemmed before index lookup.';
-- HINT mode: granule is kept if every phrase-token stem is present. The row-level recheck stems both
-- the haystack tokens and the phrase, so any morphological variant matches every stem-equivalent row.

CREATE TABLE tab
(
    id UInt64,
    val String,
    INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', postprocessor = stem(lower(val), 'en'))
)
ENGINE = MergeTree ORDER BY id;

-- Three rows share the stem-'run' family for the first phrase token.
INSERT INTO tab VALUES
    (1, 'running fast'),   -- stems 'run', 'fast'
    (2, 'runs fast'),      -- stems 'run', 'fast'
    (3, 'run fast'),       -- stems 'run', 'fast'
    (4, 'walking slowly'); -- stems 'walk', 'slowli'

-- Each phrase stems to ['run','fast'], matching all three stem-'run' rows.
SELECT count() FROM tab WHERE hasPhrase(val, 'running fast');     -- 3
SELECT count() FROM tab WHERE hasPhrase(val, 'runs fast');        -- 3
SELECT count() FROM tab WHERE hasPhrase(val, 'run fast');         -- 3
-- The three phrases are equivalent after stemming; together they still cover the three stem-'run' rows.
SELECT count() FROM tab WHERE hasPhrase(val, 'running fast') OR hasPhrase(val, 'runs fast') OR hasPhrase(val, 'run fast');   -- 3
-- Different stems — granule pruned.
SELECT count() FROM tab WHERE hasPhrase(val, 'jumping high');     -- 0

DROP TABLE tab;

SELECT '-- startsWith / endsWith: prefix/suffix tokens are stemmed for the hint lookup.';
-- HINT mode: granule is kept when the first/last stem of the prefix/suffix is present.
-- Row-level startsWith / endsWith does literal prefix/suffix match.

CREATE TABLE tab
(
    id UInt64,
    val String,
    INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', postprocessor = stem(lower(val), 'en'))
)
ENGINE = MergeTree ORDER BY id;

-- Three rows start with stem-'run' family; two end with stem-'dog' family.
INSERT INTO tab VALUES
    (1, 'running fast'),   -- starts with 'run' family
    (2, 'runs fast'),      -- starts with 'run' family
    (3, 'run fast'),       -- starts with 'run' family
    (4, 'cat dog'),        -- ends with 'dog' family
    (5, 'fluffy dogs');    -- ends with 'dog' family

-- Each literal prefix matches its own row; granule kept via stem 'run' for each needle.
SELECT count() FROM tab WHERE startsWith(val, 'running');   -- 1
SELECT count() FROM tab WHERE startsWith(val, 'runs');      -- 1
SELECT count() FROM tab WHERE startsWith(val, 'run');       -- 3 (literal 'run' is a prefix of 'running', 'runs', and 'run')
-- Each literal suffix matches its row; granule kept via stem 'dog'.
SELECT count() FROM tab WHERE endsWith(val, 'dog');         -- 1
SELECT count() FROM tab WHERE endsWith(val, 'dogs');        -- 1
-- Different stem — granule pruned.
SELECT count() FROM tab WHERE startsWith(val, 'jumping');   -- 0
SELECT count() FROM tab WHERE endsWith(val, 'birds');       -- 0

DROP TABLE tab;

SELECT '-- like: HINT-mode pattern tokens are stemmed.';
-- HINT mode: granule is kept when the literal pattern's tokenized stems are present.
-- Row-level LIKE remains a literal substring/wildcard match against the column.

CREATE TABLE tab
(
    id UInt64,
    val String,
    INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', postprocessor = stem(lower(val), 'en'))
)
ENGINE = MergeTree ORDER BY id;

-- Three rows share stem 'run'.
INSERT INTO tab VALUES
    (1, 'running fast'),
    (2, 'runs fast'),
    (3, 'run fast'),
    (4, 'walking slowly');

-- Literal-substring LIKE matches per row; the index keeps the granule via stem 'run'.
SELECT count() FROM tab WHERE val LIKE '%running%';   -- 1
SELECT count() FROM tab WHERE val LIKE '%runs%';      -- 1
-- 'run' is a literal substring of 'running', 'runs', and 'run', so all three match.
SELECT count() FROM tab WHERE val LIKE '%run%';       -- 3
SELECT count() FROM tab WHERE val LIKE '%walking%';   -- 1
-- Different stem — granule pruned.
SELECT count() FROM tab WHERE val LIKE '%jumping%';   -- 0

DROP TABLE tab;

SELECT '-- mapContainsKey / mapContainsKeyLike: needle is stemmed for index on mapKeys.';
-- HINT mode: granule kept when the needle's stem is in the mapKeys index. Row-level
-- mapContainsKey is exact key match; mapContainsKeyLike is literal LIKE on the keys.

CREATE TABLE tab
(
    id UInt64,
    val Map(String, String),
    INDEX idx(mapKeys(val)) TYPE text(tokenizer = 'splitByNonAlpha', postprocessor = stem(lower(mapKeys(val)), 'en'))
) ENGINE = MergeTree ORDER BY id;

-- Three rows share key-stem 'run', one has stem 'walk'.
INSERT INTO tab VALUES
    (1, {'running': 'a'}),
    (2, {'runs':    'b'}),
    (3, {'run':     'c'}),
    (4, {'walking': 'd'});

-- Each literal key matches its own row; granule kept via stem 'run' for every needle.
SELECT count() FROM tab WHERE mapContainsKey(val, 'running');         -- 1
SELECT count() FROM tab WHERE mapContainsKey(val, 'runs');            -- 1
SELECT count() FROM tab WHERE mapContainsKey(val, 'run');             -- 1
-- Different stem — granule pruned.
SELECT count() FROM tab WHERE mapContainsKey(val, 'jumping');         -- 0
-- Literal LIKE pattern matches its row; the index keeps the granule via stem 'run'.
SELECT count() FROM tab WHERE mapContainsKeyLike(val, '%running%');   -- 1
-- 'run' is a literal substring of all three run-stem keys.
SELECT count() FROM tab WHERE mapContainsKeyLike(val, '%run%');       -- 3

DROP TABLE tab;

SELECT '-- mapContainsValue / mapContainsValueLike: needle is stemmed for index on mapValues.';
-- Same pattern as the mapContainsKey test above, but the index is built on mapValues.

CREATE TABLE tab
(
    id UInt64,
    val Map(String, String),
    INDEX idx(mapValues(val)) TYPE text(tokenizer = 'splitByNonAlpha', postprocessor = stem(lower(mapValues(val)), 'en'))
) ENGINE = MergeTree ORDER BY id;

-- Three rows share value-stem 'run', one has stem 'walk'.
INSERT INTO tab VALUES
    (1, {'a': 'running'}),
    (2, {'b': 'runs'}),
    (3, {'c': 'run'}),
    (4, {'d': 'walking'});

SELECT count() FROM tab WHERE mapContainsValue(val, 'running');         -- 1
SELECT count() FROM tab WHERE mapContainsValue(val, 'runs');            -- 1
SELECT count() FROM tab WHERE mapContainsValue(val, 'run');             -- 1
SELECT count() FROM tab WHERE mapContainsValue(val, 'jumping');         -- 0
SELECT count() FROM tab WHERE mapContainsValueLike(val, '%running%');   -- 1
SELECT count() FROM tab WHERE mapContainsValueLike(val, '%run%');       -- 3

DROP TABLE tab;
