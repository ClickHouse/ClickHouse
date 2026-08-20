-- Explicit priors are a typed collection literal: an array of (class, probability) pairs. Class ids
-- and probabilities may arrive as numeric or string scalars, and the definition round-trips through
-- the stored DDL.

DROP DICTIONARY IF EXISTS nb_priors_pairs;
DROP DICTIONARY IF EXISTS nb_priors_string_scalars;
DROP TABLE IF EXISTS nb_priors_src;

CREATE TABLE nb_priors_src (ngram String, class_id UInt32, count UInt64) ENGINE = MergeTree ORDER BY (class_id, ngram);
INSERT INTO nb_priors_src VALUES ('good', 0, 5), ('bad', 1, 5);

CREATE DICTIONARY nb_priors_pairs (ngram String, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_priors_src'))
LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token' priors_mode 'explicit' priors [(0, 0.9), (1, 0.1)]))
LIFETIME(0);

CREATE DICTIONARY nb_priors_string_scalars (ngram String, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_priors_src'))
LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token' priors_mode 'explicit' priors [('0', 0.9), ('1', '0.1')]))
LIFETIME(0);

-- An unseen token forms no known n-grams, so the class probabilities are exactly the priors.
SELECT 'array of pairs';
SELECT arrayMap(p -> (tupleElement(p, 1), round(tupleElement(p, 2), 4)), naiveBayesClassifierWithAllProbs('nb_priors_pairs', 'unseen'));
SELECT 'string scalars';
SELECT arrayMap(p -> (tupleElement(p, 1), round(tupleElement(p, 2), 4)), naiveBayesClassifierWithAllProbs('nb_priors_string_scalars', 'unseen'));

-- The priors literal survives in the stored DDL, and the dictionary reloads from it.
SELECT 'stored DDL keeps the priors literal';
SELECT create_table_query LIKE '%PRIORS [(0, 0.9), (1, 0.1)]%' FROM system.tables WHERE database = currentDatabase() AND name = 'nb_priors_pairs';
DETACH DICTIONARY nb_priors_pairs;
ATTACH DICTIONARY nb_priors_pairs;
SELECT 'after reload';
SELECT arrayMap(p -> (tupleElement(p, 1), round(tupleElement(p, 2), 4)), naiveBayesClassifierWithAllProbs('nb_priors_pairs', 'unseen'));

DROP DICTIONARY nb_priors_pairs;
DROP DICTIONARY nb_priors_string_scalars;
DROP TABLE nb_priors_src;
