-- LowCardinality(Nullable(String)) inputs across all three functions. A NULL value yields NULL for the scalar and
-- single-probability variants; naiveBayesClassifierWithAllProbs yields an empty array, matching plain Nullable(String)
-- (an array result cannot be wrapped in Nullable). Non-NULL values classify normally.

DROP DICTIONARY IF EXISTS nb_lc;
DROP TABLE IF EXISTS nb_lc_src;

CREATE TABLE nb_lc_src (ngram String, class_id UInt32, count UInt64) ENGINE = Memory;
INSERT INTO nb_lc_src VALUES ('good', 0, 10), ('bad', 1, 10);

CREATE DICTIONARY nb_lc (ngram String, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram
SOURCE(CLICKHOUSE(TABLE 'nb_lc_src'))
LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token'))
LIFETIME(0);

-- NULL scalar: classifier and single-probability variant return NULL; the all-probabilities variant returns an empty array.
WITH CAST(NULL AS LowCardinality(Nullable(String))) AS x
SELECT
    naiveBayesClassifier('nb_lc', x),
    naiveBayesClassifierWithProb('nb_lc', x),
    naiveBayesClassifierWithAllProbs('nb_lc', x);

-- Return types for the NULL scalar.
WITH CAST(NULL AS LowCardinality(Nullable(String))) AS x
SELECT
    toTypeName(naiveBayesClassifier('nb_lc', x)),
    toTypeName(naiveBayesClassifierWithProb('nb_lc', x)),
    toTypeName(naiveBayesClassifierWithAllProbs('nb_lc', x));

-- A non-NULL LowCardinality(Nullable(String)) value classifies normally.
WITH CAST('bad' AS LowCardinality(Nullable(String))) AS x
SELECT naiveBayesClassifier('nb_lc', x), naiveBayesClassifierWithProb('nb_lc', x).1;

-- A column that mixes NULL and non-NULL values.
DROP TABLE IF EXISTS nb_lc_rows;
CREATE TABLE nb_lc_rows (id UInt32, x LowCardinality(Nullable(String))) ENGINE = Memory;
INSERT INTO nb_lc_rows VALUES (0, 'good'), (1, NULL), (2, 'bad');
SELECT id, naiveBayesClassifier('nb_lc', x), naiveBayesClassifierWithAllProbs('nb_lc', x) FROM nb_lc_rows ORDER BY id;

DROP TABLE nb_lc_rows;
DROP DICTIONARY nb_lc;
DROP TABLE nb_lc_src;
