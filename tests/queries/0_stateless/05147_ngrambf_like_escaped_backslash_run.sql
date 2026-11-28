DROP TABLE IF EXISTS t_ngram_backslash;

-- The SQL literal 'xxab\\\\cdyy' is the value `xxab\\cdyy` (two literal backslashes) and the pattern
-- literal '%ab\\\\\\\\%cd%' is `%ab\\\\%cd%`: `ab`, two literal backslashes, a wildcard, `cd`.
-- The n-grams the index requires must all be substrings of every matching value.

CREATE TABLE t_ngram_backslash (s String, INDEX i s TYPE ngrambf_v1(2, 512, 2, 0) GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 2;

-- 'xxabcdyy' is the same value without the backslashes and lives in another granule: a pattern that
-- requires them must neither match that value nor keep its granule.

INSERT INTO t_ngram_backslash VALUES ('xxab\\\\cdyy'), ('other1'), ('xxabcdyy'), ('other2'), ('other3');

SELECT count() FROM t_ngram_backslash WHERE s LIKE '%ab\\\\\\\\%cd%' SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_ngram_backslash WHERE s LIKE '%ab\\\\\\\\%cd%';

SELECT count() FROM t_ngram_backslash WHERE s LIKE '%ab%cd%';
SELECT count() FROM t_ngram_backslash WHERE s LIKE '%ab\\\\%cd%';
SELECT count() FROM t_ngram_backslash WHERE s LIKE '%xxab%';
SELECT count() FROM t_ngram_backslash WHERE s LIKE '%other%';

-- The same two literal backslashes reached through a custom ESCAPE character: under ESCAPE '!' a
-- backslash is literal, and both index conditions fold the escape character into a backslash-escaped
-- pattern before tokenizing it, so '%ab\\\\%cd%' here is the pattern above.

SELECT count() FROM t_ngram_backslash WHERE s LIKE '%ab\\\\%cd%' ESCAPE '!' SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_ngram_backslash WHERE s LIKE '%ab\\\\%cd%' ESCAPE '!';

-- Two agreeing counts are also what an index that prunes nothing gives, so the granule count index
-- analysis reached is asserted as well; `use_skip_indexes_on_data_read` keeps the filtering at that
-- stage, and the assertion fails by producing no row at all if the index is not applied.
-- `EXPLAIN` renders the plan as a tree by default, and the tree prefix of a step that is not a last
-- child carries box-drawing characters rather than spaces, so extract the counter instead of
-- trimming the indentation: under parallel replicas the read step gains a sibling.

SELECT extract(explain, 'Granules: [0-9]+/[0-9]+') FROM (
    EXPLAIN indexes = 1 SELECT count() FROM t_ngram_backslash WHERE s LIKE '%ab\\\\%cd%' ESCAPE '!'
    SETTINGS use_skip_indexes_on_data_read = 0
) WHERE explain LIKE '%Granules: %/%';

DROP TABLE t_ngram_backslash;

-- With an n-gram size of 1 a single literal backslash before the wildcard is already enough.

CREATE TABLE t_ngram_backslash (s String, INDEX i s TYPE ngrambf_v1(1, 512, 2, 0) GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 2;

INSERT INTO t_ngram_backslash VALUES ('xxab\\cdyy'), ('other1'), ('other2'), ('other3');

SELECT count() FROM t_ngram_backslash WHERE s LIKE '%ab\\\\%cd%' SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_ngram_backslash WHERE s LIKE '%ab\\\\%cd%';
SELECT count() FROM t_ngram_backslash WHERE s LIKE '%ab\\\\%cd%' SETTINGS force_data_skipping_indices = 'i';

DROP TABLE t_ngram_backslash;

-- The same tokenizer answers the `LIKE` fallback of a text index with the `ngrams` tokenizer.

DROP TABLE IF EXISTS t_text_index_backslash;

CREATE TABLE t_text_index_backslash (s String, INDEX i s TYPE text(tokenizer = ngrams(2)) GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 2;

INSERT INTO t_text_index_backslash VALUES ('xxab\\\\cdyy'), ('other1'), ('xxabcdyy'), ('other2'), ('other3');

SELECT count() FROM t_text_index_backslash WHERE s LIKE '%ab\\\\\\\\%cd%' SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_text_index_backslash WHERE s LIKE '%ab\\\\\\\\%cd%'
SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 0;

SELECT count() FROM t_text_index_backslash WHERE s LIKE '%ab\\\\%cd%' ESCAPE '!' SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_text_index_backslash WHERE s LIKE '%ab\\\\%cd%' ESCAPE '!'
SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 0;
SELECT extract(explain, 'Granules: [0-9]+/[0-9]+') FROM (
    EXPLAIN indexes = 1 SELECT count() FROM t_text_index_backslash WHERE s LIKE '%ab\\\\%cd%' ESCAPE '!'
    SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 0, use_skip_indexes_on_data_read = 0
) WHERE explain LIKE '%Granules: %/%';

DROP TABLE t_text_index_backslash;
