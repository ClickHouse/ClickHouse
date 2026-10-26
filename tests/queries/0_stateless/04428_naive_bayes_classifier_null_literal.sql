-- A bare NULL literal (Nullable(Nothing)) is accepted by all three classifier functions and classifies to NULL,
-- the same as a typed Nullable(String) NULL.

DROP DICTIONARY IF EXISTS nb_null;
DROP TABLE IF EXISTS nb_null_src;

CREATE TABLE nb_null_src (ngram String, class_id UInt32, count UInt64) ENGINE = Memory;
INSERT INTO nb_null_src VALUES ('good', 0, 10), ('bad', 1, 10);

CREATE DICTIONARY nb_null (ngram String, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram
SOURCE(CLICKHOUSE(TABLE 'nb_null_src'))
LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token'))
LIFETIME(0);

SELECT naiveBayesClassifier('nb_null', NULL) AS v, toTypeName(v);
SELECT naiveBayesClassifierWithProb('nb_null', NULL) AS v, toTypeName(v);
SELECT naiveBayesClassifierWithAllProbs('nb_null', NULL) AS v, toTypeName(v);

-- A bare NULL behaves like a typed Nullable(String) NULL.
SELECT naiveBayesClassifier('nb_null', CAST(NULL AS Nullable(String))) AS v, toTypeName(v);

DROP DICTIONARY nb_null;
DROP TABLE nb_null_src;
