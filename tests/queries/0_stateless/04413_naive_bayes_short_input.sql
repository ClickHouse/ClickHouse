-- With no padding (the default), an input shorter than n forms no n-grams. This must be handled gracefully:
-- the input is classified from the priors alone rather than erroring. Once the input has at least n units the
-- n-grams take over. Class 0 dominates the training counts, so the prior favors class 0; the bigram 'a b' is
-- the only class-1 evidence.

DROP TABLE IF EXISTS nb_short_src;
CREATE TABLE nb_short_src (class_id UInt32, ngram String, count UInt64) ENGINE = MergeTree ORDER BY (class_id, ngram);
INSERT INTO nb_short_src VALUES (0, 'x y', 100), (0, 'z w', 100), (1, 'a b', 5);

DROP DICTIONARY IF EXISTS nb_short;
CREATE DICTIONARY nb_short (ngram String, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_short_src'))
LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 2 mode 'token' priors_mode 'proportional')) LIFETIME(0);

-- Empty and sub-n inputs are classified (not rejected) and fall back to the prior (class 0); an input with
-- at least n=2 tokens uses the matching bigram (class 1).
SELECT 'empty', naiveBayesClassifier('nb_short', '');
SELECT 'one-token', naiveBayesClassifier('nb_short', 'a');
SELECT 'two-token', naiveBayesClassifier('nb_short', 'a b');

-- A sub-n input contributes no n-grams, so its class probabilities are exactly the priors: a one-token input
-- and the empty input produce identical probabilities.
SELECT 'sub-n equals prior',
       arrayMap(p -> (p.1, round(p.2, 6)), naiveBayesClassifierWithAllProbs('nb_short', 'a'))
     = arrayMap(p -> (p.1, round(p.2, 6)), naiveBayesClassifierWithAllProbs('nb_short', ''));

-- dictGet agrees with the function for a sub-n input.
SELECT 'dictget agrees', naiveBayesClassifier('nb_short', 'a') = dictGet('nb_short', 'class_id', 'a');

DROP DICTIONARY nb_short;
DROP TABLE nb_short_src;
