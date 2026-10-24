SET allow_experimental_projection_text_index = 1;

-- Test hasPhrase with projection text index and enable_phrase_query_support.
-- Covers: single part, merge, re-merge, triple merge, embedded/large posting
-- transitions, repeated words, repeated tokens in phrase, long phrase,
-- dense/rare tokens, hasToken regression, fallback when phrase support absent.

SET enable_full_text_index = 1;

-- ═══════════════════════════════════════════════════════════════
-- SECTION 1: Basic single-part correctness
-- ═══════════════════════════════════════════════════════════════
DROP TABLE IF EXISTS t_phrase;
CREATE TABLE t_phrase (id UInt32, text String,
    PROJECTION idx INDEX text TYPE text(tokenizer = 'splitByNonAlpha',
        posting_list_codec = 'bitpacking', enable_phrase_query_support = 1)
) ENGINE = MergeTree ORDER BY id;

INSERT INTO t_phrase VALUES
    (1, 'the quick brown fox jumps over the lazy dog'),
    (2, 'a quick movement of the fox'),
    (3, 'brown bear sat on the log'),
    (4, 'the fox is quick and brown');

SELECT '-- S1: exact phrase match';
SELECT id FROM t_phrase WHERE hasPhrase(text, 'quick brown') ORDER BY id;

SELECT '-- S1: four-token phrase';
SELECT id FROM t_phrase WHERE hasPhrase(text, 'the quick brown fox') ORDER BY id;

SELECT '-- S1: phrase at end';
SELECT id FROM t_phrase WHERE hasPhrase(text, 'lazy dog') ORDER BY id;

SELECT '-- S1: reversed order → no match';
SELECT id FROM t_phrase WHERE hasPhrase(text, 'brown quick') ORDER BY id;

SELECT '-- S1: nonexistent tokens';
SELECT id FROM t_phrase WHERE hasPhrase(text, 'nonexistent phrase') ORDER BY id;

SELECT '-- S1: single token phrase (degenerates to hasToken)';
SELECT id FROM t_phrase WHERE hasPhrase(text, 'fox') ORDER BY id;

SELECT '-- S1: hasToken regression check';
SELECT id FROM t_phrase WHERE hasToken(text, 'quick') ORDER BY id;

-- ═══════════════════════════════════════════════════════════════
-- SECTION 2: Two-part merge (L0 + L0 → L1)
-- ═══════════════════════════════════════════════════════════════
INSERT INTO t_phrase VALUES
    (5, 'quick brown foxes leap over lazy dogs'),
    (6, 'the brown quick fox stood still'),
    (7, 'over the lazy river the quick brown fox ran');

SELECT '-- S2: before merge';
SELECT id FROM t_phrase WHERE hasPhrase(text, 'quick brown') ORDER BY id;

OPTIMIZE TABLE t_phrase FINAL;

SELECT '-- S2: after merge';
SELECT id FROM t_phrase WHERE hasPhrase(text, 'quick brown') ORDER BY id;

SELECT '-- S2: lazy dog after merge';
SELECT id FROM t_phrase WHERE hasPhrase(text, 'lazy dog') ORDER BY id;

SELECT '-- S2: brown quick after merge';
SELECT id FROM t_phrase WHERE hasPhrase(text, 'brown quick') ORDER BY id;

SELECT '-- S2: hasToken after merge';
SELECT id FROM t_phrase WHERE hasToken(text, 'quick') ORDER BY id;

-- ═══════════════════════════════════════════════════════════════
-- SECTION 3: Re-merge (L1 + L0 → L2), embedded→large transition
-- ═══════════════════════════════════════════════════════════════
INSERT INTO t_phrase VALUES
    (8, 'the quick brown fox again'),
    (9, 'another lazy dog day');

OPTIMIZE TABLE t_phrase FINAL;

SELECT '-- S3: quick brown after re-merge';
SELECT id FROM t_phrase WHERE hasPhrase(text, 'quick brown') ORDER BY id;

SELECT '-- S3: lazy dog after re-merge';
SELECT id FROM t_phrase WHERE hasPhrase(text, 'lazy dog') ORDER BY id;

SELECT '-- S3: four-token after re-merge';
SELECT id FROM t_phrase WHERE hasPhrase(text, 'the quick brown fox') ORDER BY id;

-- ═══════════════════════════════════════════════════════════════
-- SECTION 4: Triple merge (L2 + L0 → L3), large→large
-- ═══════════════════════════════════════════════════════════════
INSERT INTO t_phrase VALUES
    (10, 'quick brown snake');

OPTIMIZE TABLE t_phrase FINAL;

SELECT '-- S4: quick brown after triple merge';
SELECT id FROM t_phrase WHERE hasPhrase(text, 'quick brown') ORDER BY id;

SELECT '-- S4: hasToken after triple merge';
SELECT id FROM t_phrase WHERE hasToken(text, 'brown') ORDER BY id;

DROP TABLE t_phrase;

-- ═══════════════════════════════════════════════════════════════
-- SECTION 5: Repeated words in doc (multi-position per doc)
-- ═══════════════════════════════════════════════════════════════
DROP TABLE IF EXISTS t_repeat;
CREATE TABLE t_repeat (id UInt32, text String,
    PROJECTION idx INDEX text TYPE text(tokenizer = 'splitByNonAlpha',
        posting_list_codec = 'bitpacking', enable_phrase_query_support = 1)
) ENGINE = MergeTree ORDER BY id;

INSERT INTO t_repeat VALUES
    (1, 'the the the quick brown'),
    (2, 'quick the quick brown fox'),
    (3, 'brown quick brown fox brown'),
    (4, 'a b c d e');

SELECT '-- S5: phrase "the quick" with repeated "the"';
SELECT id FROM t_repeat WHERE hasPhrase(text, 'the quick') ORDER BY id;

SELECT '-- S5: phrase "quick brown" with repeated tokens';
SELECT id FROM t_repeat WHERE hasPhrase(text, 'quick brown') ORDER BY id;

SELECT '-- S5: phrase "brown fox" with repeated "brown"';
SELECT id FROM t_repeat WHERE hasPhrase(text, 'brown fox') ORDER BY id;

-- Repeated tokens in phrase itself
SELECT '-- S5: phrase "the the" (consecutive repeated)';
SELECT id FROM t_repeat WHERE hasPhrase(text, 'the the') ORDER BY id;

SELECT '-- S5: phrase "the the the" (triple repeated)';
SELECT id FROM t_repeat WHERE hasPhrase(text, 'the the the') ORDER BY id;

SELECT '-- S5: phrase "the the the quick" ';
SELECT id FROM t_repeat WHERE hasPhrase(text, 'the the the quick') ORDER BY id;

-- Test after merge
INSERT INTO t_repeat VALUES
    (5, 'the the quick brown fox'),
    (6, 'quick brown quick brown');

OPTIMIZE TABLE t_repeat FINAL;

SELECT '-- S5: "the the quick" after merge';
SELECT id FROM t_repeat WHERE hasPhrase(text, 'the the quick') ORDER BY id;

SELECT '-- S5: "quick brown" after merge';
SELECT id FROM t_repeat WHERE hasPhrase(text, 'quick brown') ORDER BY id;

DROP TABLE t_repeat;

-- ═══════════════════════════════════════════════════════════════
-- SECTION 6: Dense token (appears in every doc)
-- ═══════════════════════════════════════════════════════════════
DROP TABLE IF EXISTS t_dense;
CREATE TABLE t_dense (id UInt32, text String,
    PROJECTION idx INDEX text TYPE text(tokenizer = 'splitByNonAlpha',
        posting_list_codec = 'bitpacking', enable_phrase_query_support = 1)
) ENGINE = MergeTree ORDER BY id;

INSERT INTO t_dense VALUES
    (1, 'x a b'), (2, 'x b c'), (3, 'x c a'), (4, 'x a c');

INSERT INTO t_dense VALUES
    (5, 'x a b'), (6, 'x c b'), (7, 'x a b'), (8, 'x b a');

OPTIMIZE TABLE t_dense FINAL;

SELECT '-- S6: "x a" after merge (dense x, sparse a)';
SELECT id FROM t_dense WHERE hasPhrase(text, 'x a') ORDER BY id;

SELECT '-- S6: "a b" after merge';
SELECT id FROM t_dense WHERE hasPhrase(text, 'a b') ORDER BY id;

SELECT '-- S6: "b a" after merge';
SELECT id FROM t_dense WHERE hasPhrase(text, 'b a') ORDER BY id;

-- Re-merge to push x into large posting (>6 docs → 9)
INSERT INTO t_dense VALUES (9, 'x a b');
OPTIMIZE TABLE t_dense FINAL;

SELECT '-- S6: "x a" after re-merge (x=9 docs, large)';
SELECT id FROM t_dense WHERE hasPhrase(text, 'x a') ORDER BY id;

DROP TABLE t_dense;

-- ═══════════════════════════════════════════════════════════════
-- SECTION 7: Three-part single merge (L0+L0+L0 → L1)
-- ═══════════════════════════════════════════════════════════════
DROP TABLE IF EXISTS t_3way;
CREATE TABLE t_3way (id UInt32, text String,
    PROJECTION idx INDEX text TYPE text(tokenizer = 'splitByNonAlpha',
        posting_list_codec = 'bitpacking', enable_phrase_query_support = 1)
) ENGINE = MergeTree ORDER BY id;

INSERT INTO t_3way VALUES (1, 'quick brown a'), (2, 'brown quick b');
INSERT INTO t_3way VALUES (3, 'quick brown c');
INSERT INTO t_3way VALUES (4, 'quick brown d'), (5, 'other words');

OPTIMIZE TABLE t_3way FINAL;

SELECT '-- S7: three-part merge';
SELECT id FROM t_3way WHERE hasPhrase(text, 'quick brown') ORDER BY id;

DROP TABLE t_3way;

-- ═══════════════════════════════════════════════════════════════
-- SECTION 8: Large posting in single insert (>6 docs, one part)
-- ═══════════════════════════════════════════════════════════════
DROP TABLE IF EXISTS t_large;
CREATE TABLE t_large (id UInt32, text String,
    PROJECTION idx INDEX text TYPE text(tokenizer = 'splitByNonAlpha',
        posting_list_codec = 'bitpacking', enable_phrase_query_support = 1)
) ENGINE = MergeTree ORDER BY id;

INSERT INTO t_large VALUES
    (1, 'quick brown a'), (2, 'quick brown b'), (3, 'quick brown c'),
    (4, 'quick brown d'), (5, 'quick brown e'), (6, 'quick brown f'),
    (7, 'quick brown g'), (8, 'brown quick h'), (9, 'other words');

SELECT '-- S8: large posting single part';
SELECT id FROM t_large WHERE hasPhrase(text, 'quick brown') ORDER BY id;

-- Merge with another part
INSERT INTO t_large VALUES (10, 'quick brown i');
OPTIMIZE TABLE t_large FINAL;

SELECT '-- S8: large posting after merge';
SELECT id FROM t_large WHERE hasPhrase(text, 'quick brown') ORDER BY id;

DROP TABLE t_large;

-- ═══════════════════════════════════════════════════════════════
-- SECTION 9: Long phrase (5+ tokens)
-- ═══════════════════════════════════════════════════════════════
DROP TABLE IF EXISTS t_long;
CREATE TABLE t_long (id UInt32, text String,
    PROJECTION idx INDEX text TYPE text(tokenizer = 'splitByNonAlpha',
        posting_list_codec = 'bitpacking', enable_phrase_query_support = 1)
) ENGINE = MergeTree ORDER BY id;

INSERT INTO t_long VALUES
    (1, 'a b c d e f g h'),
    (2, 'x a b c d e f y'),
    (3, 'a b c x d e f g'),
    (4, 'z a b c d e f g h i');

SELECT '-- S9: five-token phrase';
SELECT id FROM t_long WHERE hasPhrase(text, 'a b c d e') ORDER BY id;

SELECT '-- S9: six-token phrase';
SELECT id FROM t_long WHERE hasPhrase(text, 'a b c d e f') ORDER BY id;

SELECT '-- S9: seven-token phrase (only exact match)';
SELECT id FROM t_long WHERE hasPhrase(text, 'a b c d e f g') ORDER BY id;

SELECT '-- S9: eight-token phrase';
SELECT id FROM t_long WHERE hasPhrase(text, 'a b c d e f g h') ORDER BY id;

INSERT INTO t_long VALUES (5, 'a b c d e f g h');
OPTIMIZE TABLE t_long FINAL;

SELECT '-- S9: after merge';
SELECT id FROM t_long WHERE hasPhrase(text, 'a b c d e f g h') ORDER BY id;

DROP TABLE t_long;

-- ═══════════════════════════════════════════════════════════════
-- SECTION 10: hasPhrase on table WITHOUT enable_phrase_query_support
--             Verifies: no crash, hasAllTokens still works after merge.
-- ═══════════════════════════════════════════════════════════════
DROP TABLE IF EXISTS t_nophrase;
CREATE TABLE t_nophrase (id UInt32, text String,
    PROJECTION idx INDEX text TYPE text(tokenizer = 'splitByNonAlpha',
        posting_list_codec = 'bitpacking')
) ENGINE = MergeTree ORDER BY id;

INSERT INTO t_nophrase VALUES
    (1, 'quick brown fox'), (2, 'brown quick fox'), (3, 'other');
INSERT INTO t_nophrase VALUES (4, 'quick brown cat');

OPTIMIZE TABLE t_nophrase FINAL;

SELECT '-- S10: hasAllTokens after merge (no phrase support)';
SELECT id FROM t_nophrase WHERE hasAllTokens(text, 'quick brown') ORDER BY id;

DROP TABLE t_nophrase;

-- ═══════════════════════════════════════════════════════════════
-- SECTION 11: hasPhrase + hasToken + hasAllTokens combined
-- ═══════════════════════════════════════════════════════════════
DROP TABLE IF EXISTS t_combined;
CREATE TABLE t_combined (id UInt32, text String,
    PROJECTION idx INDEX text TYPE text(tokenizer = 'splitByNonAlpha',
        posting_list_codec = 'bitpacking', enable_phrase_query_support = 1)
) ENGINE = MergeTree ORDER BY id;

INSERT INTO t_combined VALUES
    (1, 'quick brown fox'), (2, 'brown quick fox'),
    (3, 'quick fox brown'), (4, 'the quick brown fox');

INSERT INTO t_combined VALUES
    (5, 'quick brown cat'), (6, 'hello world');

OPTIMIZE TABLE t_combined FINAL;

SELECT '-- S11: hasPhrase AND hasToken combined';
SELECT id FROM t_combined WHERE hasPhrase(text, 'quick brown') AND hasToken(text, 'fox') ORDER BY id;

SELECT '-- S11: hasAllTokens vs hasPhrase';
SELECT id FROM t_combined WHERE hasAllTokens(text, 'quick brown') ORDER BY id;
SELECT id FROM t_combined WHERE hasPhrase(text, 'quick brown') ORDER BY id;

SELECT '-- S11: multiple hasPhrase in same query';
SELECT id FROM t_combined WHERE hasPhrase(text, 'quick brown') OR hasPhrase(text, 'hello world') ORDER BY id;

DROP TABLE t_combined;

-- ═══════════════════════════════════════════════════════════════
-- SECTION 12: Full text as phrase / empty results
-- ═══════════════════════════════════════════════════════════════
DROP TABLE IF EXISTS t_edge;
CREATE TABLE t_edge (id UInt32, text String,
    PROJECTION idx INDEX text TYPE text(tokenizer = 'splitByNonAlpha',
        posting_list_codec = 'bitpacking', enable_phrase_query_support = 1)
) ENGINE = MergeTree ORDER BY id;

INSERT INTO t_edge VALUES
    (1, 'hello world'),
    (2, 'hello'),
    (3, 'world hello');

SELECT '-- S12: full text as phrase';
SELECT id FROM t_edge WHERE hasPhrase(text, 'hello world') ORDER BY id;

SELECT '-- S12: partial overlap not matching';
SELECT id FROM t_edge WHERE hasPhrase(text, 'world hello world') ORDER BY id;

SELECT '-- S12: single word match';
SELECT id FROM t_edge WHERE hasPhrase(text, 'hello') ORDER BY id;

DROP TABLE t_edge;

-- ═══════════════════════════════════════════════════════════════
-- SECTION 13: Embedded + Embedded merge matrix
-- Case A: both ≤6, merged ≤6 (stays embedded)
-- Case B: both ≤6, merged >6 (overflow to large)
-- ═══════════════════════════════════════════════════════════════
DROP TABLE IF EXISTS t_ee;
CREATE TABLE t_ee (id UInt32, text String,
    PROJECTION idx INDEX text TYPE text(tokenizer = 'splitByNonAlpha',
        posting_list_codec = 'bitpacking', enable_phrase_query_support = 1)
) ENGINE = MergeTree ORDER BY id;

-- Part 1: token "p q" in 3 docs (embedded)
INSERT INTO t_ee VALUES (1,'p q a'),(2,'p q b'),(3,'p q c'),(4,'other x');

-- Part 2: token "p q" in 2 docs (embedded), merged total = 5 ≤ 6 → stays embedded
INSERT INTO t_ee VALUES (5,'p q d'),(6,'p q e'),(7,'other y');

OPTIMIZE TABLE t_ee FINAL;

SELECT '-- S13a: embedded+embedded stays embedded (3+2=5)';
SELECT id FROM t_ee WHERE hasPhrase(text, 'p q') ORDER BY id;

-- Re-merge: add 2 more → total 7 > 6, overflow to large
INSERT INTO t_ee VALUES (8,'p q f'),(9,'p q g');

OPTIMIZE TABLE t_ee FINAL;

SELECT '-- S13b: embedded overflow to large after re-merge (5+2=7)';
SELECT id FROM t_ee WHERE hasPhrase(text, 'p q') ORDER BY id;

DROP TABLE t_ee;

-- Case B variant: first merge itself overflows
DROP TABLE IF EXISTS t_ee2;
CREATE TABLE t_ee2 (id UInt32, text String,
    PROJECTION idx INDEX text TYPE text(tokenizer = 'splitByNonAlpha',
        posting_list_codec = 'bitpacking', enable_phrase_query_support = 1)
) ENGINE = MergeTree ORDER BY id;

-- Part 1: 4 docs with "p q"
INSERT INTO t_ee2 VALUES (1,'p q a'),(2,'p q b'),(3,'p q c'),(4,'p q d');

-- Part 2: 4 docs with "p q", merged total = 8 > 6 → overflow
INSERT INTO t_ee2 VALUES (5,'p q e'),(6,'p q f'),(7,'p q g'),(8,'p q h');

OPTIMIZE TABLE t_ee2 FINAL;

SELECT '-- S13c: embedded+embedded overflow (4+4=8)';
SELECT id FROM t_ee2 WHERE hasPhrase(text, 'p q') ORDER BY id;

SELECT '-- S13c: reversed phrase should not match';
SELECT id FROM t_ee2 WHERE hasPhrase(text, 'q p') ORDER BY id;

DROP TABLE t_ee2;

-- ═══════════════════════════════════════════════════════════════
-- SECTION 14: Embedded + Large merge (Case C: emb + large → large)
-- ═══════════════════════════════════════════════════════════════
DROP TABLE IF EXISTS t_el;
CREATE TABLE t_el (id UInt32, text String,
    PROJECTION idx INDEX text TYPE text(tokenizer = 'splitByNonAlpha',
        posting_list_codec = 'bitpacking', enable_phrase_query_support = 1)
) ENGINE = MergeTree ORDER BY id;

-- Part 1: 3 docs with "p q" (embedded)
INSERT INTO t_el VALUES (1,'p q a'),(2,'p q b'),(3,'p q c');

-- Part 2: 7 docs with "p q" (large, >6)
INSERT INTO t_el VALUES (4,'p q d'),(5,'p q e'),(6,'p q f'),(7,'p q g'),(8,'p q h'),(9,'p q i'),(10,'p q j');

OPTIMIZE TABLE t_el FINAL;

SELECT '-- S14: embedded(3) + large(7) = 10';
SELECT id FROM t_el WHERE hasPhrase(text, 'p q') ORDER BY id;

-- Re-merge with fresh part
INSERT INTO t_el VALUES (11,'p q k');
OPTIMIZE TABLE t_el FINAL;

SELECT '-- S14: after re-merge (10+1=11)';
SELECT id FROM t_el WHERE hasPhrase(text, 'p q') ORDER BY id;

DROP TABLE t_el;

-- ═══════════════════════════════════════════════════════════════
-- SECTION 15: Large + Embedded merge (Case D: large + emb → large)
-- ═══════════════════════════════════════════════════════════════
DROP TABLE IF EXISTS t_le;
CREATE TABLE t_le (id UInt32, text String,
    PROJECTION idx INDEX text TYPE text(tokenizer = 'splitByNonAlpha',
        posting_list_codec = 'bitpacking', enable_phrase_query_support = 1)
) ENGINE = MergeTree ORDER BY id;

-- Part 1: 7 docs with "p q" (large)
INSERT INTO t_le VALUES (1,'p q a'),(2,'p q b'),(3,'p q c'),(4,'p q d'),(5,'p q e'),(6,'p q f'),(7,'p q g');

-- Part 2: 2 docs with "p q" (embedded)
INSERT INTO t_le VALUES (8,'p q h'),(9,'p q i');

OPTIMIZE TABLE t_le FINAL;

SELECT '-- S15: large(7) + embedded(2) = 9';
SELECT id FROM t_le WHERE hasPhrase(text, 'p q') ORDER BY id;

-- Re-merge
INSERT INTO t_le VALUES (10,'p q j');
OPTIMIZE TABLE t_le FINAL;

SELECT '-- S15: after re-merge (9+1=10)';
SELECT id FROM t_le WHERE hasPhrase(text, 'p q') ORDER BY id;

DROP TABLE t_le;

-- ═══════════════════════════════════════════════════════════════
-- SECTION 16: Large + Large merge (Case E: large + large → large)
-- ═══════════════════════════════════════════════════════════════
DROP TABLE IF EXISTS t_ll;
CREATE TABLE t_ll (id UInt32, text String,
    PROJECTION idx INDEX text TYPE text(tokenizer = 'splitByNonAlpha',
        posting_list_codec = 'bitpacking', enable_phrase_query_support = 1)
) ENGINE = MergeTree ORDER BY id;

-- Part 1: 8 docs with "p q" (large)
INSERT INTO t_ll VALUES (1,'p q a'),(2,'p q b'),(3,'p q c'),(4,'p q d'),(5,'p q e'),(6,'p q f'),(7,'p q g'),(8,'p q h');

-- Part 2: 7 docs with "p q" (large)
INSERT INTO t_ll VALUES (9,'p q i'),(10,'p q j'),(11,'p q k'),(12,'p q l'),(13,'p q m'),(14,'p q n'),(15,'p q o');

OPTIMIZE TABLE t_ll FINAL;

SELECT '-- S16: large(8) + large(7) = 15';
SELECT id FROM t_ll WHERE hasPhrase(text, 'p q') ORDER BY id;

-- Confirm non-matching phrase
SELECT '-- S16: reversed should not match';
SELECT id FROM t_ll WHERE hasPhrase(text, 'q p') ORDER BY id;

-- Re-merge with another large
INSERT INTO t_ll VALUES (16,'p q p1'),(17,'p q p2'),(18,'p q p3'),(19,'p q p4'),(20,'p q p5'),(21,'p q p6'),(22,'p q p7');

OPTIMIZE TABLE t_ll FINAL;

SELECT '-- S16: large(15) + large(7) = 22 after re-merge';
SELECT id FROM t_ll WHERE hasPhrase(text, 'p q') ORDER BY id;

DROP TABLE t_ll;

-- ═══════════════════════════════════════════════════════════════
-- SECTION 17: Asymmetric tokens — one embedded, one large in same query
-- Token "p" is dense (many docs), token "rare" is sparse (few docs)
-- Tests that doc_index tracking works when posting lists differ greatly
-- ═══════════════════════════════════════════════════════════════
DROP TABLE IF EXISTS t_asym;
CREATE TABLE t_asym (id UInt32, text String,
    PROJECTION idx INDEX text TYPE text(tokenizer = 'splitByNonAlpha',
        posting_list_codec = 'bitpacking', enable_phrase_query_support = 1)
) ENGINE = MergeTree ORDER BY id;

-- "p" in all 8 docs (large), "rare" in only 2 docs (embedded)
INSERT INTO t_asym VALUES
    (1,'p rare x'),(2,'p y'),(3,'p z'),(4,'p w'),
    (5,'p v'),(6,'p u'),(7,'p rare t'),(8,'p s');

SELECT '-- S17: "p rare" single part (large p, embedded rare)';
SELECT id FROM t_asym WHERE hasPhrase(text, 'p rare') ORDER BY id;

INSERT INTO t_asym VALUES (9,'p rare end'),(10,'p other');

OPTIMIZE TABLE t_asym FINAL;

SELECT '-- S17: "p rare" after merge';
SELECT id FROM t_asym WHERE hasPhrase(text, 'p rare') ORDER BY id;

-- Re-merge
INSERT INTO t_asym VALUES (11,'p rare final');
OPTIMIZE TABLE t_asym FINAL;

SELECT '-- S17: "p rare" after re-merge';
SELECT id FROM t_asym WHERE hasPhrase(text, 'p rare') ORDER BY id;

SELECT '-- S17: "rare p" should not match';
SELECT id FROM t_asym WHERE hasPhrase(text, 'rare p') ORDER BY id;

DROP TABLE t_asym;
