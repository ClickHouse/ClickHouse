DROP TABLE IF EXISTS t_ngram_backslash;

-- The SQL literal 'xxab\\\\cdyy' is the value `xxab\\cdyy` (two literal backslashes) and the pattern
-- literal '%ab\\\\\\\\%cd%' is `%ab\\\\%cd%`: `ab`, two literal backslashes, a wildcard, `cd`.
-- The n-grams the index requires must all be substrings of every matching value.

CREATE TABLE t_ngram_backslash (s String, INDEX i s TYPE ngrambf_v1(2, 512, 2, 0) GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 2;

INSERT INTO t_ngram_backslash VALUES ('xxab\\\\cdyy'), ('other1'), ('other2'), ('other3');

SELECT count() FROM t_ngram_backslash WHERE s LIKE '%ab\\\\\\\\%cd%' SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_ngram_backslash WHERE s LIKE '%ab\\\\\\\\%cd%';

SELECT count() FROM t_ngram_backslash WHERE s LIKE '%ab%cd%';
SELECT count() FROM t_ngram_backslash WHERE s LIKE '%ab\\\\%cd%';
SELECT count() FROM t_ngram_backslash WHERE s LIKE '%xxab%';
SELECT count() FROM t_ngram_backslash WHERE s LIKE '%other%';

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

INSERT INTO t_text_index_backslash VALUES ('xxab\\\\cdyy'), ('other1'), ('other2'), ('other3');

SELECT count() FROM t_text_index_backslash WHERE s LIKE '%ab\\\\\\\\%cd%' SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_text_index_backslash WHERE s LIKE '%ab\\\\\\\\%cd%'
SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 0;

DROP TABLE t_text_index_backslash;
