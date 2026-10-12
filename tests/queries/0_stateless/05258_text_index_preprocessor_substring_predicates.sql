-- The index stores tokens of preprocessor(value). LIKE, startsWith, endsWith, match, the multi-search
-- and multi-pattern predicates and the map key/value LIKE predicates keep their SQL semantics, so the
-- index may only over-approximate them. Taking the required tokens from preprocessor(needle) is a valid
-- superset filter only when the preprocessor maps each character independently: one that deletes,
-- reorders or folds characters across positions makes the needle's tokens absent from the value's, and
-- the granule holding a matching row is pruned away.
-- Every predicate below is asserted with the index and with use_skip_indexes = 0; both must agree.
-- The cases that need Vectorscan or ICU live in 04311_text_index_non_utf8_needle_no_prune, which is
-- tagged for those libraries.

SELECT '-- substringIndex preprocessor from the documentation, at default settings';

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    s String,
    INDEX idx(s) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = substringIndex(s, '\n', 1))
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO tab VALUES ('header line\nHello there');

SELECT 'endsWith', count() FROM tab WHERE endsWith(s, 'Hello there');
SELECT 'endsWith (no index)', count() FROM tab WHERE endsWith(s, 'Hello there') SETTINGS use_skip_indexes = 0;

DROP TABLE tab;

SELECT '-- soundex preprocessor, ngrams tokenizer';

-- soundex folds a whole word into a four-character code, so no character of the needle survives in
-- place: 'hello, world!' becomes H464 while the needle 'hello' becomes H400.

CREATE TABLE tab
(
    s String,
    INDEX idx(s) TYPE text(tokenizer = ngrams(2), preprocessor = soundex(lower(s)))
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO tab VALUES ('Hello, world!');

SELECT 'like', count() FROM tab WHERE s LIKE '%Hello%';
SELECT 'like (no index)', count() FROM tab WHERE s LIKE '%Hello%' SETTINGS use_skip_indexes = 0;
SELECT 'startsWith', count() FROM tab WHERE startsWith(s, 'Hello');
SELECT 'startsWith (no index)', count() FROM tab WHERE startsWith(s, 'Hello') SETTINGS use_skip_indexes = 0;
SELECT 'endsWith', count() FROM tab WHERE endsWith(s, 'world!');
SELECT 'endsWith (no index)', count() FROM tab WHERE endsWith(s, 'world!') SETTINGS use_skip_indexes = 0;
SELECT 'match', count() FROM tab WHERE match(s, 'Hello');
SELECT 'match (no index)', count() FROM tab WHERE match(s, 'Hello') SETTINGS use_skip_indexes = 0;
SELECT 'multiSearchAny', count() FROM tab WHERE multiSearchAny(s, ['Hello']);
SELECT 'multiSearchAny (no index)', count() FROM tab WHERE multiSearchAny(s, ['Hello']) SETTINGS use_skip_indexes = 0;
SELECT 'multiSearchAnyUTF8', count() FROM tab WHERE multiSearchAnyUTF8(s, ['Hello']);
SELECT 'multiSearchAnyUTF8 (no index)', count() FROM tab WHERE multiSearchAnyUTF8(s, ['Hello']) SETTINGS use_skip_indexes = 0;

-- Refusing the atom leaves the index out of the plan entirely.
SELECT 'index in plan', countIf(explain LIKE '%idx%') > 0 FROM (EXPLAIN indexes = 1 SELECT s FROM tab WHERE s LIKE '%Hello%');

DROP TABLE tab;

SELECT '-- soundex preprocessor, splitByNonAlpha tokenizer';

CREATE TABLE tab
(
    s String,
    INDEX idx(s) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = soundex(lower(s)))
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO tab VALUES ('Hello, world!');

SELECT 'like', count() FROM tab WHERE s LIKE '%Hello%';
SELECT 'like (no index)', count() FROM tab WHERE s LIKE '%Hello%' SETTINGS use_skip_indexes = 0;

DROP TABLE tab;

SELECT '-- soundex preprocessor on a map index: mapContainsKeyLike / mapContainsValueLike';

-- An index on an array-typed expression applies the preprocessor element-wise, so the same needle
-- arithmetic holds per map key and per map value.

CREATE TABLE tabm
(
    m Map(String, String),
    INDEX idx_mk(mapKeys(m)) TYPE text(tokenizer = ngrams(2), preprocessor = soundex(lower(mapKeys(m)))),
    INDEX idx_mv(mapValues(m)) TYPE text(tokenizer = ngrams(2), preprocessor = soundex(lower(mapValues(m))))
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO tabm VALUES (map('Hello, world!', 'Hello, world!'));

SELECT 'mapContainsKeyLike', count() FROM tabm WHERE mapContainsKeyLike(m, '%Hello%');
SELECT 'mapContainsKeyLike (no index)', count() FROM tabm WHERE mapContainsKeyLike(m, '%Hello%') SETTINGS use_skip_indexes = 0;
SELECT 'mapContainsValueLike', count() FROM tabm WHERE mapContainsValueLike(m, '%Hello%');
SELECT 'mapContainsValueLike (no index)', count() FROM tabm WHERE mapContainsValueLike(m, '%Hello%') SETTINGS use_skip_indexes = 0;

DROP TABLE tabm;

SELECT '-- Control: ASCII lower on a map index keeps mapContainsKeyLike / mapContainsValueLike on the index';

CREATE TABLE tabm
(
    m Map(String, String),
    INDEX idx_mk(mapKeys(m)) TYPE text(tokenizer = ngrams(2), preprocessor = lower(mapKeys(m))),
    INDEX idx_mv(mapValues(m)) TYPE text(tokenizer = ngrams(2), preprocessor = lower(mapValues(m)))
)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;

INSERT INTO tabm VALUES (map('Hello, world!', 'Hello, world!')), (map('nothing', 'to see'));

SELECT 'mapContainsKeyLike', count() FROM tabm WHERE mapContainsKeyLike(m, '%Hello%');
SELECT 'mapContainsKeyLike (no index)', count() FROM tabm WHERE mapContainsKeyLike(m, '%Hello%') SETTINGS use_skip_indexes = 0;
SELECT 'mapContainsValueLike', count() FROM tabm WHERE mapContainsValueLike(m, '%Hello%');
SELECT 'mapContainsValueLike (no index)', count() FROM tabm WHERE mapContainsValueLike(m, '%Hello%') SETTINGS use_skip_indexes = 0;
-- One row per granule, so these fail if either index is refused or stops pruning the non-matching row.
SELECT 'granules pruned (keys)', countIf(explain LIKE '%Granules: 1/2%') > 0 FROM (EXPLAIN indexes = 1 SELECT m FROM tabm WHERE mapContainsKeyLike(m, '%Hello%'));
SELECT 'granules pruned (values)', countIf(explain LIKE '%Granules: 1/2%') > 0 FROM (EXPLAIN indexes = 1 SELECT m FROM tabm WHERE mapContainsValueLike(m, '%Hello%'));

DROP TABLE tabm;

SELECT '-- Control: ASCII lower maps every byte in place, so the index is still used';

-- Each control table below holds one matching and one non-matching row at index_granularity = 1. The
-- counts alone cannot tell a refused index from one that prunes, because the granule holding the match is
-- read either way; the EXPLAIN assertion is what observes the other granule being pruned.

CREATE TABLE tab
(
    s String,
    INDEX idx(s) TYPE text(tokenizer = ngrams(2), preprocessor = lower(s))
)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;

INSERT INTO tab VALUES ('Hello, world!'), ('ΣΟΣΑ');

SELECT 'like', count() FROM tab WHERE s LIKE '%Hello%';
SELECT 'like (no index)', count() FROM tab WHERE s LIKE '%Hello%' SETTINGS use_skip_indexes = 0;
SELECT 'granules pruned', countIf(explain LIKE '%Granules: 1/2%') > 0 FROM (EXPLAIN indexes = 1 SELECT s FROM tab WHERE s LIKE '%Hello%');
-- ASCII case mapping leaves non-ASCII bytes alone, so a Greek sigma needle is unaffected.
SELECT 'startsWith non-ASCII', count() FROM tab WHERE startsWith(s, 'ΣΟΣ');
SELECT 'startsWith non-ASCII (no index)', count() FROM tab WHERE startsWith(s, 'ΣΟΣ') SETTINGS use_skip_indexes = 0;
SELECT 'startsWith non-ASCII granules pruned', countIf(explain LIKE '%Granules: 1/2%') > 0 FROM (EXPLAIN indexes = 1 SELECT s FROM tab WHERE startsWith(s, 'ΣΟΣ'));
SELECT 'index in plan', countIf(explain LIKE '%idx%') > 0 FROM (EXPLAIN indexes = 1 SELECT s FROM tab WHERE s LIKE '%Hello%');

DROP TABLE tab;

SELECT '-- Control: ASCII upper is admitted on the same terms as ASCII lower';

CREATE TABLE tab
(
    s String,
    INDEX idx(s) TYPE text(tokenizer = ngrams(2), preprocessor = upper(s))
)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;

INSERT INTO tab VALUES ('Hello, world!'), ('nothing to see');

SELECT 'like', count() FROM tab WHERE s LIKE '%world%';
SELECT 'like (no index)', count() FROM tab WHERE s LIKE '%world%' SETTINGS use_skip_indexes = 0;
SELECT 'granules pruned', countIf(explain LIKE '%Granules: 1/2%') > 0 FROM (EXPLAIN indexes = 1 SELECT s FROM tab WHERE s LIKE '%world%');
SELECT 'index in plan', countIf(explain LIKE '%idx%') > 0 FROM (EXPLAIN indexes = 1 SELECT s FROM tab WHERE s LIKE '%world%');

DROP TABLE tab;

SELECT '-- Control: without a preprocessor nothing changes';

CREATE TABLE tab
(
    s String,
    INDEX idx(s) TYPE text(tokenizer = ngrams(2))
)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;

INSERT INTO tab VALUES ('Hello, world!'), ('nothing to see');

SELECT 'like', count() FROM tab WHERE s LIKE '%Hello%';
SELECT 'like (no index)', count() FROM tab WHERE s LIKE '%Hello%' SETTINGS use_skip_indexes = 0;
SELECT 'granules pruned', countIf(explain LIKE '%Granules: 1/2%') > 0 FROM (EXPLAIN indexes = 1 SELECT s FROM tab WHERE s LIKE '%Hello%');
SELECT 'index in plan', countIf(explain LIKE '%idx%') > 0 FROM (EXPLAIN indexes = 1 SELECT s FROM tab WHERE s LIKE '%Hello%');

DROP TABLE tab;
