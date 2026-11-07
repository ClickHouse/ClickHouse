-- A tokenizer made of whole code points (`ngrams`, `sparseGrams`, `asciiCJK`) must not prune with a LIKE pattern or a needle that is not valid UTF-8.

SET enable_full_text_index = 1;

DROP TABLE IF EXISTS ngram;
DROP TABLE IF EXISTS ngram_map;
DROP TABLE IF EXISTS sparse;
DROP TABLE IF EXISTS text_ngram;
DROP TABLE IF EXISTS text_map;
DROP TABLE IF EXISTS text_sparse;
DROP TABLE IF EXISTS text_cjk;
DROP TABLE IF EXISTS text_preproc;

SELECT 'ngrambf_v1';

CREATE TABLE ngram (id UInt32, s String, INDEX idx s TYPE ngrambf_v1(3, 512, 2, 0))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;

INSERT INTO ngram VALUES (1, '\xC3\xA9bc'), (2, 'ab\xC3\xA9'), (3, 'hello world');

SELECT arraySort(groupArray(id)) FROM ngram WHERE s LIKE '%\xA9bc%';
SELECT arraySort(groupArray(id)) FROM ngram WHERE s LIKE '%\xA9bc%' SETTINGS use_skip_indexes = 0;
SELECT arraySort(groupArray(id)) FROM ngram WHERE s LIKE '%ab\xC3%';
SELECT arraySort(groupArray(id)) FROM ngram WHERE s LIKE '%ab\xC3%' SETTINGS use_skip_indexes = 0;
SELECT arraySort(groupArray(id)) FROM ngram WHERE startsWith(s, 'ab\xC3');
SELECT arraySort(groupArray(id)) FROM ngram WHERE startsWith(s, 'ab\xC3') SETTINGS use_skip_indexes = 0;
SELECT arraySort(groupArray(id)) FROM ngram WHERE endsWith(s, '\xA9bc');
SELECT arraySort(groupArray(id)) FROM ngram WHERE endsWith(s, '\xA9bc') SETTINGS use_skip_indexes = 0;
SELECT arraySort(groupArray(id)) FROM ngram WHERE multiSearchAny(s, ['\xA9bc']);
SELECT arraySort(groupArray(id)) FROM ngram WHERE multiSearchAny(s, ['\xA9bc']) SETTINGS use_skip_indexes = 0;
SELECT arraySort(groupArray(id)) FROM ngram WHERE multiSearchAny(s, ['nomatch', '\xA9bc']);
SELECT arraySort(groupArray(id)) FROM ngram WHERE multiSearchAny(s, ['nomatch', '\xA9bc']) SETTINGS use_skip_indexes = 0;
SELECT arraySort(groupArray(id)) FROM ngram WHERE match(s, '\xA9bc');
SELECT arraySort(groupArray(id)) FROM ngram WHERE match(s, '\xA9bc') SETTINGS use_skip_indexes = 0;
SELECT arraySort(groupArray(id)) FROM ngram WHERE match(s, 'nomatch|\xA9bc');
SELECT arraySort(groupArray(id)) FROM ngram WHERE match(s, 'nomatch|\xA9bc') SETTINGS use_skip_indexes = 0;
SELECT arraySort(groupArray(id)) FROM ngram WHERE NOT (s NOT LIKE '%\xA9bc%');
SELECT arraySort(groupArray(id)) FROM ngram WHERE NOT (s NOT LIKE '%\xA9bc%') SETTINGS use_skip_indexes = 0;

SELECT extract(explain, 'Granules: [0-9]+/[0-9]+') AS explain FROM (
    EXPLAIN indexes = 1 SELECT count() FROM ngram WHERE s LIKE '%\xA9bc%'
) WHERE explain LIKE '%Granules: %/%';
SELECT extract(explain, 'Granules: [0-9]+/[0-9]+') AS explain FROM (
    EXPLAIN indexes = 1 SELECT count() FROM ngram WHERE s LIKE '%hello%'
) WHERE explain LIKE '%Granules: %/%';

SELECT 'ngrambf_v1 on map keys and values';

CREATE TABLE ngram_map (
    id UInt32,
    m Map(String, String),
    INDEX ik mapKeys(m) TYPE ngrambf_v1(3, 512, 2, 0),
    INDEX iv mapValues(m) TYPE ngrambf_v1(3, 512, 2, 0)
)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;

INSERT INTO ngram_map VALUES (1, {'\xC3\xA9bc': '\xC3\xA9bc'}), (2, {'ab\xC3\xA9': 'ab\xC3\xA9'}), (3, {'hello': 'world'});

SELECT arraySort(groupArray(id)) FROM ngram_map WHERE mapContainsKeyLike(m, '%\xA9bc%');
SELECT arraySort(groupArray(id)) FROM ngram_map WHERE mapContainsKeyLike(m, '%\xA9bc%') SETTINGS use_skip_indexes = 0;
SELECT arraySort(groupArray(id)) FROM ngram_map WHERE mapContainsValueLike(m, '%\xA9bc%');
SELECT arraySort(groupArray(id)) FROM ngram_map WHERE mapContainsValueLike(m, '%\xA9bc%') SETTINGS use_skip_indexes = 0;

SELECT extract(explain, 'Granules: [0-9]+/[0-9]+') AS explain FROM (
    EXPLAIN indexes = 1 SELECT count() FROM ngram_map WHERE mapContainsKeyLike(m, '%\xA9bc%')
) WHERE explain LIKE '%Granules: %/%';
SELECT extract(explain, 'Granules: [0-9]+/[0-9]+') AS explain FROM (
    EXPLAIN indexes = 1 SELECT count() FROM ngram_map WHERE mapContainsKeyLike(m, '%hello%')
) WHERE explain LIKE '%Granules: %/%';

SELECT 'sparse_grams';

CREATE TABLE sparse (id UInt32, s String, INDEX idx s TYPE sparse_grams(3, 100, 512, 2, 0))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;

INSERT INTO sparse VALUES (1, '\xC3\xA9abcdefgh'), (2, 'abcdef\xC3\xA9'), (3, 'hello world');

SELECT arraySort(groupArray(id)) FROM sparse WHERE s LIKE '%\xA9abcdefgh%';
SELECT arraySort(groupArray(id)) FROM sparse WHERE s LIKE '%\xA9abcdefgh%' SETTINGS use_skip_indexes = 0;
SELECT arraySort(groupArray(id)) FROM sparse WHERE s LIKE '%abcdef\xC3%';
SELECT arraySort(groupArray(id)) FROM sparse WHERE s LIKE '%abcdef\xC3%' SETTINGS use_skip_indexes = 0;
SELECT arraySort(groupArray(id)) FROM sparse WHERE multiSearchAny(s, ['\xA9abcdefgh']);
SELECT arraySort(groupArray(id)) FROM sparse WHERE multiSearchAny(s, ['\xA9abcdefgh']) SETTINGS use_skip_indexes = 0;

SELECT extract(explain, 'Granules: [0-9]+/[0-9]+') AS explain FROM (
    EXPLAIN indexes = 1 SELECT count() FROM sparse WHERE s LIKE '%\xA9abcdefgh%'
) WHERE explain LIKE '%Granules: %/%';
SELECT extract(explain, 'Granules: [0-9]+/[0-9]+') AS explain FROM (
    EXPLAIN indexes = 1 SELECT count() FROM sparse WHERE s LIKE '%hello world%'
) WHERE explain LIKE '%Granules: %/%';

SELECT 'text index with ngrams';

CREATE TABLE text_ngram (id UInt32, s String, INDEX idx s TYPE text(tokenizer = ngrams(3)))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;

INSERT INTO text_ngram VALUES (1, '\xC3\xA9bc'), (2, 'ab\xC3\xA9'), (3, 'hello world');

SELECT arraySort(groupArray(id)) FROM text_ngram WHERE s LIKE '%\xA9bc%';
SELECT arraySort(groupArray(id)) FROM text_ngram WHERE s LIKE '%\xA9bc%' SETTINGS use_skip_indexes = 0;
SELECT arraySort(groupArray(id)) FROM text_ngram WHERE s LIKE '%ab\xC3%';
SELECT arraySort(groupArray(id)) FROM text_ngram WHERE s LIKE '%ab\xC3%' SETTINGS use_skip_indexes = 0;

SELECT extract(explain, 'Granules: [0-9]+/[0-9]+') AS explain FROM (
    EXPLAIN indexes = 1 SELECT count() FROM text_ngram WHERE s LIKE '%\xA9bc%'
) WHERE explain LIKE '%Granules: %/%';
SELECT extract(explain, 'Granules: [0-9]+/[0-9]+') AS explain FROM (
    EXPLAIN indexes = 1 SELECT count() FROM text_ngram WHERE s LIKE '%hello%'
) WHERE explain LIKE '%Granules: %/%';

SELECT 'text index with ngrams on map keys and values';

CREATE TABLE text_map (
    id UInt32,
    m Map(String, String),
    INDEX ik mapKeys(m) TYPE text(tokenizer = ngrams(3)),
    INDEX iv mapValues(m) TYPE text(tokenizer = ngrams(3))
)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;

INSERT INTO text_map VALUES (1, {'\xC3\xA9bc': '\xC3\xA9bc'}), (2, {'ab\xC3\xA9': 'ab\xC3\xA9'}), (3, {'hello': 'world'});

SELECT arraySort(groupArray(id)) FROM text_map WHERE mapContainsKeyLike(m, '%\xA9bc%');
SELECT arraySort(groupArray(id)) FROM text_map WHERE mapContainsKeyLike(m, '%\xA9bc%') SETTINGS use_skip_indexes = 0;
SELECT arraySort(groupArray(id)) FROM text_map WHERE mapContainsValueLike(m, '%\xA9bc%');
SELECT arraySort(groupArray(id)) FROM text_map WHERE mapContainsValueLike(m, '%\xA9bc%') SETTINGS use_skip_indexes = 0;

SELECT extract(explain, 'Granules: [0-9]+/[0-9]+') AS explain FROM (
    EXPLAIN indexes = 1 SELECT count() FROM text_map WHERE mapContainsKeyLike(m, '%\xA9bc%')
) WHERE explain LIKE '%Granules: %/%';
SELECT extract(explain, 'Granules: [0-9]+/[0-9]+') AS explain FROM (
    EXPLAIN indexes = 1 SELECT count() FROM text_map WHERE mapContainsKeyLike(m, '%hello%')
) WHERE explain LIKE '%Granules: %/%';

SELECT 'text index with sparseGrams';

CREATE TABLE text_sparse (id UInt32, s String, INDEX idx s TYPE text(tokenizer = sparseGrams))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;

INSERT INTO text_sparse VALUES (1, '\xC3\xA9abcdefgh'), (2, 'abcdef\xC3\xA9'), (3, 'hello world');

SELECT arraySort(groupArray(id)) FROM text_sparse WHERE s LIKE '%\xA9abcdefgh%';
SELECT arraySort(groupArray(id)) FROM text_sparse WHERE s LIKE '%\xA9abcdefgh%' SETTINGS use_skip_indexes = 0;
SELECT arraySort(groupArray(id)) FROM text_sparse WHERE s LIKE '%abcdef\xC3%';
SELECT arraySort(groupArray(id)) FROM text_sparse WHERE s LIKE '%abcdef\xC3%' SETTINGS use_skip_indexes = 0;

SELECT extract(explain, 'Granules: [0-9]+/[0-9]+') AS explain FROM (
    EXPLAIN indexes = 1 SELECT count() FROM text_sparse WHERE s LIKE '%\xA9abcdefgh%'
) WHERE explain LIKE '%Granules: %/%';
SELECT extract(explain, 'Granules: [0-9]+/[0-9]+') AS explain FROM (
    EXPLAIN indexes = 1 SELECT count() FROM text_sparse WHERE s LIKE '%hello world%'
) WHERE explain LIKE '%Granules: %/%';

SELECT 'text index with asciiCJK';

CREATE TABLE text_cjk (id UInt32, s String, INDEX idx s TYPE text(tokenizer = asciiCJK))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;

INSERT INTO text_cjk VALUES (1, '\xC3\xA9ab xyz'), (2, 'ab \xC3\xA9'), (3, 'hello world');

SELECT arraySort(groupArray(id)) FROM text_cjk WHERE s LIKE '%\xA9ab %';
SELECT arraySort(groupArray(id)) FROM text_cjk WHERE s LIKE '%\xA9ab %' SETTINGS use_skip_indexes = 0;
SELECT arraySort(groupArray(id)) FROM text_cjk WHERE s LIKE '%ab \xC3%';
SELECT arraySort(groupArray(id)) FROM text_cjk WHERE s LIKE '%ab \xC3%' SETTINGS use_skip_indexes = 0;

SELECT extract(explain, 'Granules: [0-9]+/[0-9]+') AS explain FROM (
    EXPLAIN indexes = 1 SELECT count() FROM text_cjk WHERE s LIKE '%\xA9ab %'
) WHERE explain LIKE '%Granules: %/%';
SELECT extract(explain, 'Granules: [0-9]+/[0-9]+') AS explain FROM (
    EXPLAIN indexes = 1 SELECT count() FROM text_cjk WHERE s LIKE 'hello world'
) WHERE explain LIKE '%Granules: %/%';

SELECT 'text index with ngrams and a toValidUTF8 preprocessor';

CREATE TABLE text_preproc (id UInt32, s String, INDEX idx s TYPE text(tokenizer = ngrams(3), preprocessor = toValidUTF8(s)))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;

INSERT INTO text_preproc VALUES (1, '\xC3\xA9bc'), (2, 'ab\xC3\xA9'), (3, 'hello world');

SELECT arraySort(groupArray(id)) FROM text_preproc WHERE s LIKE '%\xA9bc%';
SELECT arraySort(groupArray(id)) FROM text_preproc WHERE s LIKE '%\xA9bc%' SETTINGS use_skip_indexes = 0;
SELECT arraySort(groupArray(id)) FROM text_preproc WHERE startsWith(s, 'ab\xC3');
SELECT arraySort(groupArray(id)) FROM text_preproc WHERE startsWith(s, 'ab\xC3') SETTINGS use_skip_indexes = 0;

SELECT extract(explain, 'Granules: [0-9]+/[0-9]+') AS explain FROM (
    EXPLAIN indexes = 1 SELECT count() FROM text_preproc WHERE s LIKE '%\xA9bc%'
) WHERE explain LIKE '%Granules: %/%';
SELECT extract(explain, 'Granules: [0-9]+/[0-9]+') AS explain FROM (
    EXPLAIN indexes = 1 SELECT count() FROM text_preproc WHERE s LIKE '%hello%'
) WHERE explain LIKE '%Granules: %/%';

DROP TABLE ngram;
DROP TABLE ngram_map;
DROP TABLE sparse;
DROP TABLE text_ngram;
DROP TABLE text_map;
DROP TABLE text_sparse;
DROP TABLE text_cjk;
DROP TABLE text_preproc;
