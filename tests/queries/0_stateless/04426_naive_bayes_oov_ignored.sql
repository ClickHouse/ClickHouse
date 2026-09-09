-- Out-of-vocabulary n-grams (not seen in training) are ignored at query time: they contribute to no class, so an
-- all-OOV input classifies identically to an empty input (the prior alone), with no bias toward smaller classes.

DROP DICTIONARY IF EXISTS nb_oov_u;
DROP DICTIONARY IF EXISTS nb_oov_p;
DROP TABLE IF EXISTS nb_oov_src;

CREATE TABLE nb_oov_src (ngram String, class_id UInt32, count UInt64) ENGINE = Memory;
-- Class 0 has a far larger total n-gram count than class 1.
INSERT INTO nb_oov_src VALUES ('a', 0, 100), ('b', 0, 100), ('c', 0, 100), ('d', 1, 1);

CREATE DICTIONARY nb_oov_u (ngram String, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_oov_src'))
LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token' priors_mode 'uniform')) LIFETIME(0);

CREATE DICTIONARY nb_oov_p (ngram String, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_oov_src'))
LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token' priors_mode 'proportional')) LIFETIME(0);

-- An all-OOV input reduces to the prior, exactly like an empty input, under either prior mode.
SELECT naiveBayesClassifier('nb_oov_u', 'zzz yyy xxx') = naiveBayesClassifier('nb_oov_u', '');
SELECT naiveBayesClassifierWithAllProbs('nb_oov_u', 'zzz yyy xxx') = naiveBayesClassifierWithAllProbs('nb_oov_u', '');
SELECT naiveBayesClassifier('nb_oov_p', 'zzz yyy xxx') = naiveBayesClassifier('nb_oov_p', '');

-- The prediction follows the prior, not the smaller class: uniform picks the lowest class id, proportional the larger class.
SELECT naiveBayesClassifier('nb_oov_u', 'zzz yyy xxx'), naiveBayesClassifier('nb_oov_p', 'zzz yyy xxx');

-- An OOV n-gram mixed with an in-vocabulary one is ignored: 'a zzz' classifies the same as 'a'.
SELECT naiveBayesClassifier('nb_oov_u', 'a zzz') = naiveBayesClassifier('nb_oov_u', 'a'), naiveBayesClassifier('nb_oov_u', 'a');

DROP DICTIONARY nb_oov_u;
DROP DICTIONARY nb_oov_p;
DROP TABLE nb_oov_src;
