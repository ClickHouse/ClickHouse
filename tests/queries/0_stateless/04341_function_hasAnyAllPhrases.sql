SELECT 'Negative tests';
-- Must accept two to three arguments
SELECT hasAnyPhrases(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT hasAnyPhrases('a'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT hasAnyPhrases('a', ['b'], 'splitByNonAlpha', 'extra'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT hasAllPhrases(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
-- 1st arg must be String or FixedString
SELECT hasAnyPhrases(1, ['hello']); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT hasAllPhrases(1, ['hello']); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
-- 2nd arg must be const Array(String)
SELECT hasAnyPhrases('a', 'b'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT hasAnyPhrases('a', [1, 2]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT hasAnyPhrases('a', materialize(['b'])); -- { serverError ILLEGAL_COLUMN }
-- 3rd arg (if given) must be const String (tokenizer name)
SELECT hasAnyPhrases('a', ['b'], 1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT hasAnyPhrases('a', ['b'], 'unsupported_tokenizer'); -- { serverError BAD_ARGUMENTS }
SELECT hasAnyPhrases('a', ['b'], 'sparseGrams'); -- { serverError BAD_ARGUMENTS }
SELECT hasAnyPhrases('a', ['b'], 'array'); -- { serverError BAD_ARGUMENTS }
SELECT hasAllPhrases('a', ['b'], 'sparseGrams'); -- { serverError BAD_ARGUMENTS }

SELECT 'hasAnyPhrases basic';
SELECT hasAnyPhrases('the quick brown fox jumps over the lazy dog', ['quick brown']);
SELECT hasAnyPhrases('the quick brown fox jumps over the lazy dog', ['missing', 'the lazy dog']);
SELECT hasAnyPhrases('the quick brown fox jumps over the lazy dog', ['missing one', 'missing two']);
SELECT hasAnyPhrases('the quick brown fox jumps over the lazy dog', ['quick fox', 'lazy dog']);
SELECT hasAnyPhrases('the quick brown fox jumps over the lazy dog', ['quick fox', 'brown quick']);

SELECT 'hasAllPhrases basic';
SELECT hasAllPhrases('the quick brown fox jumps over the lazy dog', ['quick brown', 'lazy dog']);
SELECT hasAllPhrases('the quick brown fox jumps over the lazy dog', ['quick brown', 'missing']);
SELECT hasAllPhrases('the quick brown fox jumps over the lazy dog', ['quick brown', 'fox jumps', 'the lazy dog']);
SELECT hasAllPhrases('the quick brown fox jumps over the lazy dog', ['quick brown', 'quick fox']);

SELECT 'Edge cases';
-- empty array -> 0 for both
SELECT hasAnyPhrases('the quick brown fox', []);
SELECT hasAllPhrases('the quick brown fox', []);
-- empty phrase never matches: dropped from Any, makes All always 0
SELECT hasAnyPhrases('the quick brown fox', ['', 'quick brown']);
SELECT hasAllPhrases('the quick brown fox', ['', 'quick brown']);
SELECT hasAnyPhrases('the quick brown fox', ['', '']);
SELECT hasAllPhrases('the quick brown fox', ['quick brown', '']);
-- duplicate phrases are deduplicated, result unchanged
SELECT hasAnyPhrases('the quick brown fox', ['quick brown', 'quick brown']);
SELECT hasAllPhrases('the quick brown fox', ['quick brown', 'quick brown']);
-- single-element array
SELECT hasAnyPhrases('the quick brown fox', ['quick brown']);
SELECT hasAllPhrases('the quick brown fox', ['quick fox']);

SELECT 'hasAnyPhrase / hasAllPhrase aliases';
SELECT hasAnyPhrase('the quick brown fox', ['lazy', 'quick brown']);
SELECT hasAllPhrase('the quick brown fox', ['quick brown', 'brown fox']);

SELECT 'Equivalence with OR/AND of hasPhrase (expect all 1)';
WITH 'the quick brown fox jumps over the lazy dog' AS s
SELECT
    hasAnyPhrases(s, ['quick brown', 'lazy dog'])   = (hasPhrase(s, 'quick brown') OR hasPhrase(s, 'lazy dog')),
    hasAnyPhrases(s, ['nope', 'still nope'])         = (hasPhrase(s, 'nope') OR hasPhrase(s, 'still nope')),
    hasAnyPhrases(s, ['quick fox', 'fox jumps'])     = (hasPhrase(s, 'quick fox') OR hasPhrase(s, 'fox jumps')),
    hasAllPhrases(s, ['quick brown', 'lazy dog'])    = (hasPhrase(s, 'quick brown') AND hasPhrase(s, 'lazy dog')),
    hasAllPhrases(s, ['quick brown', 'missing'])     = (hasPhrase(s, 'quick brown') AND hasPhrase(s, 'missing')),
    hasAllPhrases(s, ['the quick', 'lazy dog', 'fox jumps']) = (hasPhrase(s, 'the quick') AND hasPhrase(s, 'lazy dog') AND hasPhrase(s, 'fox jumps'));

SELECT 'Equivalence over a column (expect no rows)';
DROP TABLE IF EXISTS tab;
CREATE TABLE tab (id UInt64, message String) ENGINE = MergeTree() ORDER BY id;
INSERT INTO tab VALUES
    (1, 'the quick brown fox jumps over the lazy dog'),
    (2, 'a fast red car drove past the old house'),
    (3, 'clickhouse is a fast analytical database'),
    (4, 'the brown quick fox'),
    (5, 'quick brown foxes are fast'),
    (6, 'the lazy dog sleeps'),
    (7, '');
SELECT id
FROM tab
WHERE hasAnyPhrases(message, ['quick brown', 'lazy dog']) != (hasPhrase(message, 'quick brown') OR hasPhrase(message, 'lazy dog'))
   OR hasAllPhrases(message, ['quick brown', 'fast'])      != (hasPhrase(message, 'quick brown') AND hasPhrase(message, 'fast'))
   OR hasAnyPhrases(message, ['fast', 'brown quick', 'old house']) != (hasPhrase(message, 'fast') OR hasPhrase(message, 'brown quick') OR hasPhrase(message, 'old house'))
ORDER BY id;

SELECT 'hasAnyPhrases / hasAllPhrases over a column';
SELECT id, hasAnyPhrases(message, ['lazy dog', 'fast analytical']) AS a, hasAllPhrases(message, ['the', 'quick brown']) AS b FROM tab ORDER BY id;

DROP TABLE tab;

SELECT 'FixedString input';
SELECT hasAnyPhrases(toFixedString('the quick brown fox', 19), ['lazy', 'quick brown']);
SELECT hasAllPhrases(toFixedString('the quick brown fox', 19), ['quick brown', 'brown fox']);

SELECT 'Non-const input';
SELECT hasAnyPhrases(materialize('the quick brown fox'), ['lazy', 'quick brown']);
SELECT hasAllPhrases(materialize('the quick brown fox'), ['quick brown', 'brown fox']);

SELECT 'Nullable(String) input';
SELECT hasAnyPhrases(toNullable('the quick brown fox'), ['quick brown']);
SELECT hasAnyPhrases(CAST(NULL AS Nullable(String)), ['quick brown']);
SELECT hasAllPhrases(toNullable('the quick brown fox'), ['quick brown', 'brown fox']);
SELECT hasAllPhrases(CAST(NULL AS Nullable(String)), ['quick brown']);

SELECT 'Nullable(String) column values (Any/All match OR/AND of hasPhrase, expect equal columns 1)';
DROP TABLE IF EXISTS tab;
CREATE TABLE tab (id UInt64, message Nullable(String)) ENGINE = MergeTree() ORDER BY id;
INSERT INTO tab VALUES (1, 'the quick brown fox'), (2, NULL), (3, 'quick brown eyes'), (4, NULL), (5, 'no match here');
SELECT
    id,
    hasAnyPhrases(message, ['quick brown', 'no match']) AS a,
    (hasPhrase(message, 'quick brown') OR hasPhrase(message, 'no match')) AS b,
    a IS NOT DISTINCT FROM b AS equal_any,
    hasAllPhrases(message, ['quick', 'brown']) AS c,
    (hasPhrase(message, 'quick') AND hasPhrase(message, 'brown')) AS d,
    c IS NOT DISTINCT FROM d AS equal_all
FROM tab ORDER BY id;
DROP TABLE tab;

SELECT 'splitByString tokenizer';
SELECT hasAnyPhrases('one::two::three::four', ['two::four', 'three::four'], 'splitByString([\'::\'])');
SELECT hasAllPhrases('one::two::three::four', ['two::three', 'three::four'], 'splitByString([\'::\'])');
SELECT hasAnyPhrases('()a()bc()d()', ['a()d', 'bc()d'], 'splitByString([\'()\'])');

SELECT 'ngrams tokenizer';
SELECT hasAnyPhrases('abcdef', ['xyz', 'bcd'], 'ngrams(3)');
SELECT hasAllPhrases('abcdef', ['bcd', 'cde'], 'ngrams(3)');
SELECT hasAllPhrases('abcdef', ['abc', 'xyz'], 'ngrams(3)');
