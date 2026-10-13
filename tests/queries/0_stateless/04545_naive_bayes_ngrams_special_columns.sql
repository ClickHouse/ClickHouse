-- naiveBayesNgrams over Nullable, LowCardinality, and Sparse text columns. The function returns Array(String),
-- which cannot be wrapped in Nullable, so the framework's default implementation for NULLs applies the
-- f(default(input)) convention: a NULL row is tokenized as an empty string. LowCardinality and Sparse inputs
-- are unwrapped to plain columns by the default implementations and produce the same n-grams as plain String.

-- ---- NULL literal and Nullable(String) ----
SELECT naiveBayesNgrams(NULL, 2, 'token');                                   -- bare NULL literal: NULL result
SELECT toTypeName(naiveBayesNgrams(NULL, 2, 'token'));
SELECT naiveBayesNgrams(CAST(NULL AS Nullable(String)), 2, 'token');
SELECT naiveBayesNgrams(CAST('a b' AS Nullable(String)), 2, 'token');
SELECT toTypeName(naiveBayesNgrams(materialize(CAST('a b' AS Nullable(String))), 2, 'token'));

-- A column that mixes NULL and non-NULL values: NULL rows are tokenized as empty strings, so they produce no
-- n-grams without padding and padding-only n-grams with padding (exactly as '' does).
DROP TABLE IF EXISTS nb_ngrams_nullable;
CREATE TABLE nb_ngrams_nullable (id UInt32, x Nullable(String)) ENGINE = Memory;
INSERT INTO nb_ngrams_nullable VALUES (0, 'one two'), (1, NULL), (2, ''), (3, 'three');
SELECT id, naiveBayesNgrams(x, 2, 'token') FROM nb_ngrams_nullable ORDER BY id;
SELECT id, naiveBayesNgrams(x, 2, 'token', '<s>', '</s>') FROM nb_ngrams_nullable ORDER BY id;
DROP TABLE nb_ngrams_nullable;

-- ---- LowCardinality(String) and LowCardinality(Nullable(String)) ----
SELECT naiveBayesNgrams(CAST('a b' AS LowCardinality(String)), 2, 'token');
SELECT naiveBayesNgrams(materialize(CAST('a b' AS LowCardinality(String))), 2, 'token');
SELECT toTypeName(naiveBayesNgrams(materialize(CAST('a b' AS LowCardinality(String))), 2, 'token'));
SELECT naiveBayesNgrams(materialize(CAST('a b' AS LowCardinality(String))), 2, 'token')
     = naiveBayesNgrams(materialize('a b'), 2, 'token');

DROP TABLE IF EXISTS nb_ngrams_lc;
CREATE TABLE nb_ngrams_lc (id UInt32, x LowCardinality(Nullable(String))) ENGINE = Memory;
INSERT INTO nb_ngrams_lc VALUES (0, 'one two'), (1, NULL), (2, 'one two');
SELECT id, naiveBayesNgrams(x, 2, 'token') FROM nb_ngrams_lc ORDER BY id;
DROP TABLE nb_ngrams_lc;

-- ---- Sparse serialization ----
-- A mostly-default String column read from a wide part comes back in sparse serialization; the default
-- implementation for sparse columns materializes it to full before execution.
DROP TABLE IF EXISTS nb_ngrams_sparse;
CREATE TABLE nb_ngrams_sparse (id UInt32, x String)
ENGINE = MergeTree ORDER BY id
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.5, min_bytes_for_wide_part = 0;
INSERT INTO nb_ngrams_sparse SELECT number, if(number = 5000, 'one two three', '') FROM numbers(10000);
SELECT countIf(serialization_kind = 'Sparse') > 0 FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'nb_ngrams_sparse' AND column = 'x' AND active;
SELECT sum(length(naiveBayesNgrams(x, 2, 'token'))) FROM nb_ngrams_sparse;
SELECT naiveBayesNgrams(x, 2, 'token') FROM nb_ngrams_sparse WHERE id = 5000;
DROP TABLE nb_ngrams_sparse;
