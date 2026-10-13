-- The naiveBayesClassifier* functions require a dictionary with the NAIVE_BAYES layout. Pointing them at a
-- dictionary that exists but uses a different layout is rejected with BAD_ARGUMENTS, distinct from the
-- "dictionary not found" error for an unknown name. A regular dictGet against the same dictionary still works.

DROP DICTIONARY IF EXISTS nb_not_nb;
DROP TABLE IF EXISTS nb_not_nb_src;

CREATE TABLE nb_not_nb_src (id UInt64, val String) ENGINE = Memory;
INSERT INTO nb_not_nb_src VALUES (1, 'hello');

CREATE DICTIONARY nb_not_nb (id UInt64, val String DEFAULT '')
PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 'nb_not_nb_src')) LAYOUT(FLAT()) LIFETIME(0);

-- The dictionary loads fine for its own layout.
SELECT dictGet('nb_not_nb', 'val', toUInt64(1));

-- But the Naive Bayes functions reject it because it is not a NAIVE_BAYES dictionary.
SELECT naiveBayesClassifier('nb_not_nb', 'hello'); -- { serverError BAD_ARGUMENTS }
SELECT naiveBayesClassifierWithProb('nb_not_nb', 'hello'); -- { serverError BAD_ARGUMENTS }
SELECT naiveBayesClassifierWithAllProbs('nb_not_nb', 'hello'); -- { serverError BAD_ARGUMENTS }

DROP DICTIONARY nb_not_nb;
DROP TABLE nb_not_nb_src;
