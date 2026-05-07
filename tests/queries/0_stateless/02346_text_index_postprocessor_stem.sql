-- Tags: no-fasttest
-- Tag no-fasttest: depends on libstemmer_c

-- Tests the text index postprocessor with the stem() function.
-- The postprocessor normalizes each token to its stem, enabling matching
-- of different conjugations and derivations of the same word root.
-- Covers: String, Array(String), Nullable(String) column types,
-- English / Spanish / Russian languages, regular and irregular verbs,
-- multi-word phrases, hasAllTokens / hasAnyTokens, and index inspection.

DROP TABLE IF EXISTS tab;

SELECT '1. English stem: different conjugations share the same posting list entry.';

-- Postprocessor: lower() folds case, stem() maps every word form to its root.
CREATE TABLE tab
(
    id UInt64,
    val String,
    INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', postprocessor = stem(lower(val), 'en'))
)
ENGINE = MergeTree ORDER BY id;

-- Three forms of 'run', two of 'study', two of 'collect' — all stored under one stem each.
INSERT INTO tab VALUES
    (1, 'running'),     -- stem 'run'
    (2, 'runs'),        -- stem 'run'
    (3, 'run'),         -- stem 'run'
    (4, 'studied'),     -- stem 'studi'
    (5, 'studying'),    -- stem 'studi'
    (6, 'collection'),  -- stem 'collect'
    (7, 'collecting');  -- stem 'collect'

-- Any form of 'run' reaches all three run-stem rows.
SELECT count() FROM tab WHERE hasToken(val, 'running');     -- needle→'run';    rows 1,2,3 = 3
SELECT count() FROM tab WHERE hasToken(val, 'run');         -- needle→'run';    3
SELECT count() FROM tab WHERE hasToken(val, 'runs');        -- needle→'run';    3
-- Any form of 'study' reaches both study-stem rows.
SELECT count() FROM tab WHERE hasToken(val, 'studied');     -- needle→'studi';  rows 4,5 = 2
SELECT count() FROM tab WHERE hasToken(val, 'study');       -- needle→'studi';  2 (form not stored, same stem)
-- Any form of 'collect' reaches both collect-stem rows.
SELECT count() FROM tab WHERE hasToken(val, 'collected');   -- needle→'collect'; rows 6,7 = 2
SELECT count() FROM tab WHERE hasToken(val, 'collects');    -- needle→'collect'; 2
-- 'walker' stems to 'walker' — no row uses that stem.
SELECT count() FROM tab WHERE hasToken(val, 'walker');      -- 0
-- hasAllTokens: no single row has both 'run' and 'studi'.
SELECT count() FROM tab WHERE hasAllTokens(val, 'running studied');    -- 0
-- hasAnyTokens: rows 1-3 (stem 'run') + rows 4-5 (stem 'studi') = 5.
SELECT count() FROM tab WHERE hasAnyTokens(val, ['running', 'studied']);
-- hasAnyTokens: rows 4-5 (stem 'studi') + rows 6-7 (stem 'collect') = 4.
SELECT count() FROM tab WHERE hasAnyTokens(val, ['studying', 'collecting']);

DROP TABLE tab;

SELECT '2. Spanish stem: verb conjugations (trabajar / hablar).';

CREATE TABLE tab
(
    id UInt64,
    val String,
    INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', postprocessor = stem(lower(val), 'es'))
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO tab VALUES
    (1, 'trabajar'),    -- stem 'trabaj'
    (2, 'trabajando'),  -- stem 'trabaj'
    (3, 'trabaja'),     -- stem 'trabaj'
    (4, 'hablar'),      -- stem 'habl'
    (5, 'hablando'),    -- stem 'habl'
    (6, 'habla');       -- stem 'habl'

-- All trabajar conjugations — including forms not in the index — match via stem 'trabaj'.
SELECT count() FROM tab WHERE hasToken(val, 'trabajar');    -- stem 'trabaj'; rows 1,2,3 = 3
SELECT count() FROM tab WHERE hasToken(val, 'trabajamos');  -- stem 'trabaj'; 3 (form not stored)
SELECT count() FROM tab WHERE hasToken(val, 'trabajan');    -- stem 'trabaj'; 3
-- All hablar conjugations — including forms not in the index — match via stem 'habl'.
SELECT count() FROM tab WHERE hasToken(val, 'hablar');      -- stem 'habl';   rows 4,5,6 = 3
SELECT count() FROM tab WHERE hasToken(val, 'hablaron');    -- stem 'habl';   3 (form not stored)
-- 'correr' → stem 'corr'; no row carries that stem.
SELECT count() FROM tab WHERE hasToken(val, 'correr');      -- 0
-- No single row has both 'trabaj' and 'habl'.
SELECT count() FROM tab WHERE hasAllTokens(val, 'trabajar hablar');   -- 0
-- Either stem covers all six rows.
SELECT count() FROM tab WHERE hasAnyTokens(val, ['trabajar', 'hablar']);  -- 6

DROP TABLE tab;

SELECT '3. Russian stem: verb conjugations (читать / писать), all-lowercase Cyrillic.';
-- Note: lower() is ASCII-only; Russian tokens must already be lowercase in the stored data.

CREATE TABLE tab
(
    id UInt64,
    val String,
    INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', postprocessor = stem(lower(val), 'ru'))
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO tab VALUES
    (1, 'читать'),    -- stem 'чита'
    (2, 'читает'),    -- stem 'чита'
    (3, 'читаем'),    -- stem 'чита'
    (4, 'читали'),    -- stem 'чита'
    (5, 'писать'),    -- stem 'писа'
    (6, 'писал'),     -- stem 'писа'
    (7, 'писали');    -- stem 'писа'

-- All читать forms reach the same four rows, including 'читаю' which was not inserted.
SELECT count() FROM tab WHERE hasToken(val, 'читать');   -- stem 'чита'; rows 1-4 = 4
SELECT count() FROM tab WHERE hasToken(val, 'читает');   -- stem 'чита'; 4
SELECT count() FROM tab WHERE hasToken(val, 'читаю');    -- stem 'чита'; 4 (form not stored)
-- All писать forms reach the same three rows.
SELECT count() FROM tab WHERE hasToken(val, 'писать');   -- stem 'писа'; rows 5-7 = 3
SELECT count() FROM tab WHERE hasToken(val, 'писал');    -- stem 'писа'; 3
-- 'бежать' → stem 'бежа'; not in table.
SELECT count() FROM tab WHERE hasToken(val, 'бежать');   -- 0
-- No single row has both 'чита' and 'писа'.
SELECT count() FROM tab WHERE hasAllTokens(val, 'читать писать');           -- 0
-- Either stem spans all seven rows.
SELECT count() FROM tab WHERE hasAnyTokens(val, ['читает', 'писали']);      -- 7

DROP TABLE tab;

SELECT '4. Multi-word phrases: hasAllTokens requires all stems in the same row.';
-- Different rows contain different verb conjugations. Stemming unifies each form
-- to its root, so any conjugation of a regular verb matches the right rows.
-- The irregular past tense "ran" does NOT stem to "run" — demonstrating the limit.

CREATE TABLE tab
(
    id UInt64,
    val String,
    INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', postprocessor = stem(lower(val), 'en'))
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO tab VALUES
    (1, 'the student is studying and collecting data'),
    (2, 'she studies every morning'),
    (3, 'he collected all the samples'),
    (4, 'they are running and collecting insects'),
    (5, 'she ran a study on collecting methods');

-- 'studying' → 'studi': rows 1,2,5 = 3
SELECT count() FROM tab WHERE hasToken(val, 'studying');
-- 'collecting' → 'collect': rows 1,3,4,5 = 4
SELECT count() FROM tab WHERE hasToken(val, 'collecting');
-- 'running' → 'run': row 4 only = 1
SELECT count() FROM tab WHERE hasToken(val, 'running');
-- 'ran' → 'ran' (irregular past — different stem from 'run'): row 5 only = 1
SELECT count() FROM tab WHERE hasToken(val, 'ran');
-- hasAllTokens: both 'studi' AND 'collect' in same row: rows 1 and 5 = 2
SELECT count() FROM tab WHERE hasAllTokens(val, 'studying collecting');
-- hasAllTokens: 'run' AND 'collect' in same row: row 4 = 1
SELECT count() FROM tab WHERE hasAllTokens(val, 'running collecting');
-- hasAllTokens: 'run' AND 'studi' — no row has both stems: 0
SELECT count() FROM tab WHERE hasAllTokens(val, 'running studying');
-- hasAnyTokens: either stem found across all five rows: 5
SELECT count() FROM tab WHERE hasAnyTokens(val, ['studying', 'collecting']);

DROP TABLE tab;

SELECT '5. Irregular English verbs: Snowball does not always unify all forms.';
-- Regular verbs (study, collect) map every conjugation to a single stem.
-- Irregular verbs (go, run) may produce different stems for different tenses:
--   go/going → 'go', but goes → 'goe', went → 'went', gone → 'gone'
--   run/running/runs → 'run', but ran → 'ran'

CREATE TABLE tab
(
    id UInt64,
    val String,
    INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', postprocessor = stem(lower(val), 'en'))
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO tab VALUES
    (1, 'go'),       -- stem 'go'
    (2, 'goes'),     -- stem 'goe'  ← different from 'go'
    (3, 'going'),    -- stem 'go'
    (4, 'went'),     -- stem 'went' ← different (irregular past)
    (5, 'gone'),     -- stem 'gone' ← different (past participle)
    (6, 'run'),      -- stem 'run'
    (7, 'running'),  -- stem 'run'
    (8, 'ran');      -- stem 'ran'  ← different from 'run' (irregular past)

-- 'go' and 'going' share stem 'go': rows 1,3 = 2
SELECT count() FROM tab WHERE hasToken(val, 'go');
SELECT count() FROM tab WHERE hasToken(val, 'going');
-- 'goes' → 'goe': only row 2 = 1  (third-person singular has its own stem)
SELECT count() FROM tab WHERE hasToken(val, 'goes');
-- Irregular pasts each have a unique stem:
SELECT count() FROM tab WHERE hasToken(val, 'went');   -- 1
SELECT count() FROM tab WHERE hasToken(val, 'gone');   -- 1
-- 'run'/'running'/'runs' share stem 'run': rows 6,7 = 2
SELECT count() FROM tab WHERE hasToken(val, 'running');
-- 'ran' does NOT unify with 'run': only row 8 = 1
SELECT count() FROM tab WHERE hasToken(val, 'ran');
-- hasAnyTokens: all five go-family forms hit their respective rows = 5
SELECT count() FROM tab WHERE hasAnyTokens(val, ['go', 'going', 'goes', 'went', 'gone']);

DROP TABLE tab;

SELECT '6. Irregular Spanish verb tener: only some forms share a stem.';
-- Regular -ar verbs (trabajar) map all conjugations to a single stem.
-- The irregular verb tener produces at least four different stems:
--   tener/teniendo/tenemos → 'ten'
--   tengo → 'teng'  (1st person singular — its own stem)
--   tiene → 'tien'  (3rd person singular — its own stem)
--   tuve/tuvo/tuvieron → 'tuv'  (preterite forms share a stem)

CREATE TABLE tab
(
    id UInt64,
    val String,
    INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', postprocessor = stem(lower(val), 'es'))
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO tab VALUES
    (1, 'tener'),     -- stem 'ten'
    (2, 'teniendo'),  -- stem 'ten'
    (3, 'tenemos'),   -- stem 'ten'
    (4, 'tengo'),     -- stem 'teng' ← different
    (5, 'tiene'),     -- stem 'tien' ← different
    (6, 'tuve'),      -- stem 'tuv'
    (7, 'tuvo'),      -- stem 'tuv'
    (8, 'tuvieron');  -- stem 'tuv'

-- Infinitive / gerund / 1st-person-plural share stem 'ten': rows 1,2,3 = 3
SELECT count() FROM tab WHERE hasToken(val, 'tener');
SELECT count() FROM tab WHERE hasToken(val, 'tenemos');
-- 1st and 3rd person singular have their own unique stems:
SELECT count() FROM tab WHERE hasToken(val, 'tengo');      -- 'teng'; row 4 = 1
SELECT count() FROM tab WHERE hasToken(val, 'tiene');      -- 'tien'; row 5 = 1
-- Preterite forms share stem 'tuv': rows 6,7,8 = 3
SELECT count() FROM tab WHERE hasToken(val, 'tuve');
SELECT count() FROM tab WHERE hasToken(val, 'tuvieron');
-- hasAnyTokens across all four stem groups: all 8 rows
SELECT count() FROM tab WHERE hasAnyTokens(val, ['tener', 'tengo', 'tiene', 'tuve']);

DROP TABLE tab;

SELECT '7. Array(String) column: postprocessor stems each array element.';
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

SELECT '8. Nullable(String) column: NULL values are skipped by the index.';

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

SELECT '9. Index inspection: the index stores only stemmed tokens, not original words.';
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

SELECT '10. val IN (...) routes set elements through the stem postprocessor.';
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

SELECT '11. equals: needle is stemmed before index lookup so morphologically equivalent rows are not pruned.';
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

SELECT '12. hasPhrase: phrase tokens are stemmed before index lookup.';
-- HINT mode: granule is kept if every phrase-token stem is present. Row-level hasPhrase
-- still does literal phrase matching, so each variant matches only its own row.

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

-- Each literal phrase matches its row; granule kept via stems 'run', 'fast'.
SELECT count() FROM tab WHERE hasPhrase(val, 'running fast');     -- 1
SELECT count() FROM tab WHERE hasPhrase(val, 'runs fast');        -- 1
SELECT count() FROM tab WHERE hasPhrase(val, 'run fast');         -- 1
-- All three phrases together cover every stem-'run' row.
SELECT count() FROM tab WHERE hasPhrase(val, 'running fast') OR hasPhrase(val, 'runs fast') OR hasPhrase(val, 'run fast');   -- 3
-- Different stems — granule pruned.
SELECT count() FROM tab WHERE hasPhrase(val, 'jumping high');     -- 0

DROP TABLE tab;

SELECT '13. startsWith / endsWith: prefix/suffix tokens are stemmed for the hint lookup.';
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

SELECT '14. like: HINT-mode pattern tokens are stemmed.';
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

SELECT '15. mapContainsKey / mapContainsKeyLike: needle is stemmed for index on mapKeys.';
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

SELECT '16. mapContainsValue / mapContainsValueLike: needle is stemmed for index on mapValues.';
-- Same pattern as test 15, but the index is built on mapValues.

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
