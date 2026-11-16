-- naiveBayesClassifier and dictGet are equivalent, including for empty input: an empty string is
-- classified (from the priors, as it forms no n-grams) rather than rejected. This pins that equivalence for
-- an n-1 and an n-2 model, that a batch containing an empty row classifies every row, and that the
-- probability entry points also accept empty input.

DROP DICTIONARY IF EXISTS nb_empty_uni;
DROP DICTIONARY IF EXISTS nb_empty_bi;
DROP TABLE IF EXISTS nb_empty_uni_src;
DROP TABLE IF EXISTS nb_empty_bi_src;

CREATE TABLE nb_empty_uni_src (class_id UInt32, ngram String, count UInt64) ENGINE = MergeTree ORDER BY (class_id, ngram);
INSERT INTO nb_empty_uni_src VALUES (0, 'good', 10), (0, 'great', 8), (1, 'bad', 10), (1, 'awful', 6);

CREATE TABLE nb_empty_bi_src (class_id UInt32, ngram String, count UInt64) ENGINE = MergeTree ORDER BY (class_id, ngram);
INSERT INTO nb_empty_bi_src VALUES (0, 'very good', 10), (0, 'really great', 8), (1, 'very bad', 10), (1, 'really awful', 6);

CREATE DICTIONARY nb_empty_uni (ngram String, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_empty_uni_src' DB currentDatabase()))
LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token')) LIFETIME(0);

CREATE DICTIONARY nb_empty_bi (ngram String, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_empty_bi_src' DB currentDatabase()))
LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 2 mode 'token')) LIFETIME(0);

-- Empty input is classified, and the result matches dictGet for both n sizes.
SELECT naiveBayesClassifier('nb_empty_uni', '') = dictGet('nb_empty_uni', 'class_id', '');
SELECT naiveBayesClassifier('nb_empty_bi', '') = dictGet('nb_empty_bi', 'class_id', '');

-- A batch that contains an empty row classifies every row and stays equal to dictGet (no mid-batch abort).
SELECT countIf(naiveBayesClassifier('nb_empty_uni', t) != dictGet('nb_empty_uni', 'class_id', t)) = 0
FROM (SELECT arrayJoin(['good', '', 'bad', 'good great']) AS t);

-- The probability entry points accept empty input: every class is returned and the probabilities sum to 1.
SELECT length(naiveBayesClassifierWithAllProbs('nb_empty_uni', '')) = 2;
SELECT round(arraySum(arrayMap(p -> p.2, naiveBayesClassifierWithAllProbs('nb_empty_uni', ''))), 6) = 1;
SELECT (naiveBayesClassifierWithProb('nb_empty_uni', '')).1 = naiveBayesClassifier('nb_empty_uni', '');

DROP DICTIONARY nb_empty_uni;
DROP DICTIONARY nb_empty_bi;
DROP TABLE nb_empty_uni_src;
DROP TABLE nb_empty_bi_src;
