-- Test NaiveBayes dictionary layout

-- Named-tuple element access (such as x.probability) is resolved only by the analyzer.
SET enable_analyzer = 1;

-- Setup: create source table and populate with n-gram data

DROP TABLE IF EXISTS nb_source;
DROP DICTIONARY IF EXISTS nb_token_model;
DROP DICTIONARY IF EXISTS nb_byte_model;
DROP DICTIONARY IF EXISTS nb_codepoint_model;

-- Simple sentiment model: class 0 = positive, class 1 = negative
-- Token mode, n=1 (unigrams)
CREATE TABLE nb_source
(
    class_id UInt32,
    ngram String,
    count UInt64
) ENGINE = MergeTree ORDER BY (class_id, ngram);

INSERT INTO nb_source VALUES
    (0, 'good', 10), (0, 'great', 8), (0, 'excellent', 6), (0, 'love', 7), (0, 'happy', 5),
    (0, 'amazing', 4), (0, 'wonderful', 3), (0, 'best', 3), (0, 'fantastic', 2), (0, 'nice', 4),
    (1, 'bad', 10), (1, 'terrible', 8), (1, 'awful', 6), (1, 'hate', 7), (1, 'worst', 5),
    (1, 'horrible', 4), (1, 'poor', 3), (1, 'disappointing', 3), (1, 'ugly', 2), (1, 'sad', 4);

-- Basic token mode dictionary — all four interfaces

CREATE DICTIONARY nb_token_model
(
    ngram String,
    class_id UInt32 DEFAULT 0,
    count UInt64 DEFAULT 0
)
PRIMARY KEY ngram
SOURCE(CLICKHOUSE(TABLE 'nb_source'))
LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token' alpha 1.0))
LIFETIME(0);

SELECT 'Token mode — positive text';
SELECT dictGet('nb_token_model', 'class_id', 'good great excellent');
SELECT naiveBayesClassifier('nb_token_model', 'good great excellent');
SELECT (w.1, round(w.2, 4)) FROM (SELECT naiveBayesClassifierWithProb('nb_token_model', 'good great excellent') AS w);
SELECT arrayMap(p -> (p.1, round(p.2, 4)), naiveBayesClassifierWithAllProbs('nb_token_model', 'good great excellent'));

SELECT 'Token mode — negative text';
SELECT dictGet('nb_token_model', 'class_id', 'bad terrible awful');
SELECT naiveBayesClassifier('nb_token_model', 'bad terrible awful');
SELECT (w.1, round(w.2, 4)) FROM (SELECT naiveBayesClassifierWithProb('nb_token_model', 'bad terrible awful') AS w);
SELECT arrayMap(p -> (p.1, round(p.2, 4)), naiveBayesClassifierWithAllProbs('nb_token_model', 'bad terrible awful'));

-- Byte mode — all four interfaces

DROP TABLE IF EXISTS nb_byte_source;
CREATE TABLE nb_byte_source
(
    class_id UInt32,
    ngram String,
    count UInt64
) ENGINE = MergeTree ORDER BY (class_id, ngram);

INSERT INTO nb_byte_source VALUES
    (0, 'th', 20), (0, 'he', 18), (0, 'in', 15), (0, 'er', 14), (0, 'an', 13),
    (0, 'on', 10), (0, 'at', 9), (0, 'en', 8), (0, 'to', 7), (0, 'is', 6),
    (1, 'ij', 20), (1, 'aa', 18), (1, 'oo', 15), (1, 'ee', 14), (1, 'de', 13),
    (1, 'va', 10), (1, 'en', 8), (1, 'ge', 7), (1, 'te', 6), (1, 'ng', 5);

CREATE DICTIONARY nb_byte_model
(
    ngram String,
    class_id UInt32 DEFAULT 0,
    count UInt64 DEFAULT 0
)
PRIMARY KEY ngram
SOURCE(CLICKHOUSE(TABLE 'nb_byte_source'))
LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 2 mode 'byte' alpha 1.0))
LIFETIME(0);

SELECT 'Byte mode — English text';
SELECT dictGet('nb_byte_model', 'class_id', 'the weather is nice');
SELECT naiveBayesClassifier('nb_byte_model', 'the weather is nice');
SELECT (w.1, round(w.2, 4)) FROM (SELECT naiveBayesClassifierWithProb('nb_byte_model', 'the weather is nice') AS w);
SELECT arrayMap(p -> (p.1, round(p.2, 4)), naiveBayesClassifierWithAllProbs('nb_byte_model', 'the weather is nice'));

SELECT 'Byte mode — Dutch text';
SELECT dictGet('nb_byte_model', 'class_id', 'ij vind de aardappel');
SELECT naiveBayesClassifier('nb_byte_model', 'ij vind de aardappel');
SELECT (w.1, round(w.2, 4)) FROM (SELECT naiveBayesClassifierWithProb('nb_byte_model', 'ij vind de aardappel') AS w);
SELECT arrayMap(p -> (p.1, round(p.2, 4)), naiveBayesClassifierWithAllProbs('nb_byte_model', 'ij vind de aardappel'));

-- Codepoint mode — all four interfaces

DROP TABLE IF EXISTS nb_cp_source;
CREATE TABLE nb_cp_source
(
    class_id UInt32,
    ngram String,
    count UInt64
) ENGINE = MergeTree ORDER BY (class_id, ngram);

INSERT INTO nb_cp_source VALUES
    (0, 'a', 20), (0, 'e', 18), (0, 'i', 15), (0, 'o', 14), (0, 'u', 10),
    (0, 't', 12), (0, 'h', 10), (0, 'n', 9), (0, 's', 8), (0, 'r', 7),
    (1, 'а', 20), (1, 'е', 18), (1, 'и', 15), (1, 'о', 14), (1, 'у', 10),
    (1, 'т', 12), (1, 'н', 10), (1, 'с', 9), (1, 'р', 8), (1, 'к', 7);

CREATE DICTIONARY nb_codepoint_model
(
    ngram String,
    class_id UInt32 DEFAULT 0,
    count UInt64 DEFAULT 0
)
PRIMARY KEY ngram
SOURCE(CLICKHOUSE(TABLE 'nb_cp_source'))
LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'codepoint' alpha 1.0))
LIFETIME(0);

SELECT 'Codepoint mode — Latin text';
SELECT dictGet('nb_codepoint_model', 'class_id', 'this is english text');
SELECT naiveBayesClassifier('nb_codepoint_model', 'this is english text');
SELECT (w.1, round(w.2, 4)) FROM (SELECT naiveBayesClassifierWithProb('nb_codepoint_model', 'this is english text') AS w);
SELECT arrayMap(p -> (p.1, round(p.2, 4)), naiveBayesClassifierWithAllProbs('nb_codepoint_model', 'this is english text'));

SELECT 'Codepoint mode — Cyrillic text';
SELECT dictGet('nb_codepoint_model', 'class_id', 'это русский текст');
SELECT naiveBayesClassifier('nb_codepoint_model', 'это русский текст');
SELECT (w.1, round(w.2, 4)) FROM (SELECT naiveBayesClassifierWithProb('nb_codepoint_model', 'это русский текст') AS w);
SELECT arrayMap(p -> (p.1, round(p.2, 4)), naiveBayesClassifierWithAllProbs('nb_codepoint_model', 'это русский текст'));

-- Explicit priors — all four interfaces

DROP DICTIONARY IF EXISTS nb_priors_model;

CREATE DICTIONARY nb_priors_model
(
    ngram String,
    class_id UInt32 DEFAULT 0,
    count UInt64 DEFAULT 0
)
PRIMARY KEY ngram
SOURCE(CLICKHOUSE(TABLE 'nb_source'))
LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token' alpha 1.0 priors_mode 'explicit' priors [(0, 0.9), (1, 0.1)]))
LIFETIME(0);

SELECT 'Explicit priors';
SELECT dictGet('nb_priors_model', 'class_id', 'bad');
SELECT naiveBayesClassifier('nb_priors_model', 'bad');
SELECT (w.1, round(w.2, 4)) FROM (SELECT naiveBayesClassifierWithProb('nb_priors_model', 'bad') AS w);
SELECT arrayMap(p -> (p.1, round(p.2, 4)), naiveBayesClassifierWithAllProbs('nb_priors_model', 'bad'));

SELECT dictGet('nb_priors_model', 'class_id', 'good');
SELECT naiveBayesClassifier('nb_priors_model', 'good');
SELECT (w.1, round(w.2, 4)) FROM (SELECT naiveBayesClassifierWithProb('nb_priors_model', 'good') AS w);
SELECT arrayMap(p -> (p.1, round(p.2, 4)), naiveBayesClassifierWithAllProbs('nb_priors_model', 'good'));

-- Proportional priors — all four interfaces

DROP DICTIONARY IF EXISTS nb_prop_model;

CREATE DICTIONARY nb_prop_model
(
    ngram String,
    class_id UInt32 DEFAULT 0,
    count UInt64 DEFAULT 0
)
PRIMARY KEY ngram
SOURCE(CLICKHOUSE(TABLE 'nb_source'))
LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token' alpha 1.0 priors_mode 'proportional'))
LIFETIME(0);

SELECT 'Proportional priors';
SELECT dictGet('nb_prop_model', 'class_id', 'good great');
SELECT naiveBayesClassifier('nb_prop_model', 'good great');
SELECT (w.1, round(w.2, 4)) FROM (SELECT naiveBayesClassifierWithProb('nb_prop_model', 'good great') AS w);
SELECT arrayMap(p -> (p.1, round(p.2, 4)), naiveBayesClassifierWithAllProbs('nb_prop_model', 'good great'));

SELECT dictGet('nb_prop_model', 'class_id', 'bad terrible');
SELECT naiveBayesClassifier('nb_prop_model', 'bad terrible');
SELECT (w.1, round(w.2, 4)) FROM (SELECT naiveBayesClassifierWithProb('nb_prop_model', 'bad terrible') AS w);
SELECT arrayMap(p -> (p.1, round(p.2, 4)), naiveBayesClassifierWithAllProbs('nb_prop_model', 'bad terrible'));

-- Custom alpha — all four interfaces

DROP DICTIONARY IF EXISTS nb_alpha_model;

CREATE DICTIONARY nb_alpha_model
(
    ngram String,
    class_id UInt32 DEFAULT 0,
    count UInt64 DEFAULT 0
)
PRIMARY KEY ngram
SOURCE(CLICKHOUSE(TABLE 'nb_source'))
LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token' alpha 0.01))
LIFETIME(0);

SELECT 'Small alpha';
SELECT dictGet('nb_alpha_model', 'class_id', 'good');
SELECT naiveBayesClassifier('nb_alpha_model', 'good');
SELECT (w.1, round(w.2, 4)) FROM (SELECT naiveBayesClassifierWithProb('nb_alpha_model', 'good') AS w);
SELECT arrayMap(p -> (p.1, round(p.2, 4)), naiveBayesClassifierWithAllProbs('nb_alpha_model', 'good'));

SELECT dictGet('nb_alpha_model', 'class_id', 'bad');
SELECT naiveBayesClassifier('nb_alpha_model', 'bad');
SELECT (w.1, round(w.2, 4)) FROM (SELECT naiveBayesClassifierWithProb('nb_alpha_model', 'bad') AS w);
SELECT arrayMap(p -> (p.1, round(p.2, 4)), naiveBayesClassifierWithAllProbs('nb_alpha_model', 'bad'));

-- Batch classification from a table — all four interfaces

DROP TABLE IF EXISTS texts_to_classify;
CREATE TABLE texts_to_classify (text String) ENGINE = Memory;
INSERT INTO texts_to_classify VALUES ('good great love'), ('bad terrible hate'), ('excellent wonderful'), ('awful worst');

SELECT 'Batch dictGet';
SELECT text, dictGet('nb_token_model', 'class_id', text) AS class FROM texts_to_classify ORDER BY text;

SELECT 'Batch naiveBayesClassifier';
SELECT text, naiveBayesClassifier('nb_token_model', text) AS class FROM texts_to_classify ORDER BY text;

SELECT 'Batch naiveBayesClassifierWithProb';
SELECT text, (w.1, round(w.2, 4)) AS result FROM (SELECT text, naiveBayesClassifierWithProb('nb_token_model', text) AS w FROM texts_to_classify) ORDER BY text;

SELECT 'Batch naiveBayesClassifierWithAllProbs';
SELECT text, arrayMap(p -> (p.1, round(p.2, 4)), naiveBayesClassifierWithAllProbs('nb_token_model', text)) AS result FROM texts_to_classify ORDER BY text;

-- SYSTEM RELOAD DICTIONARY — all four interfaces

SELECT 'Reload dictionary';
SYSTEM RELOAD DICTIONARY nb_token_model;
SELECT dictGet('nb_token_model', 'class_id', 'good great');
SELECT naiveBayesClassifier('nb_token_model', 'good great');
SELECT (w.1, round(w.2, 4)) FROM (SELECT naiveBayesClassifierWithProb('nb_token_model', 'good great') AS w);
SELECT arrayMap(p -> (p.1, round(p.2, 4)), naiveBayesClassifierWithAllProbs('nb_token_model', 'good great'));

-- The dictHas function always returns 1

SELECT 'Membership via dictHas';
SELECT dictHas('nb_token_model', 'anything at all');
SELECT dictHas('nb_token_model', '');

-- Duplicate ngram accumulation in source data

DROP TABLE IF EXISTS nb_dup_source;
DROP DICTIONARY IF EXISTS nb_dup_model;

CREATE TABLE nb_dup_source
(
    class_id UInt32,
    ngram String,
    count UInt64
) ENGINE = MergeTree ORDER BY (class_id, ngram);

-- Same (class_id, ngram) pair inserted twice — counts should accumulate to 15
INSERT INTO nb_dup_source VALUES (0, 'yes', 10), (0, 'yes', 5), (1, 'no', 10);

CREATE DICTIONARY nb_dup_model
(
    ngram String,
    class_id UInt32 DEFAULT 0,
    count UInt64 DEFAULT 0
)
PRIMARY KEY ngram
SOURCE(CLICKHOUSE(TABLE 'nb_dup_source'))
LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token'))
LIFETIME(0);

SELECT 'Duplicate ngram accumulation';
SELECT naiveBayesClassifier('nb_dup_model', 'yes');
SELECT naiveBayesClassifier('nb_dup_model', 'no');
SELECT (w.1, round(w.2, 4)) FROM (SELECT naiveBayesClassifierWithProb('nb_dup_model', 'yes') AS w);
SELECT arrayMap(p -> (p.1, round(p.2, 4)), naiveBayesClassifierWithAllProbs('nb_dup_model', 'yes'));

DROP DICTIONARY IF EXISTS nb_dup_model;
DROP TABLE IF EXISTS nb_dup_source;

-- Probability invariants

SELECT 'Probability invariants';
-- Probabilities sum to 1.0
SELECT round(arraySum(arrayMap(x -> x.probability, naiveBayesClassifierWithAllProbs('nb_token_model', 'good great excellent'))), 2) AS prob_sum;
-- Number of classes equals 2
SELECT length(naiveBayesClassifierWithAllProbs('nb_token_model', 'good great excellent')) AS num_classes;
-- WithProb class matches plain classify
SELECT
    naiveBayesClassifier('nb_token_model', 'good great') = (naiveBayesClassifierWithProb('nb_token_model', 'good great')).class_id AS class_matches;
-- WithProb probability > 0.5 for clear-cut inputs
SELECT (naiveBayesClassifierWithProb('nb_token_model', 'good great excellent')).probability > 0.5 AS high_prob;

-- The store_source option enables reading the training rows back

DROP DICTIONARY IF EXISTS nb_stored_model;
DROP TABLE IF EXISTS nb_stored_source;

CREATE TABLE nb_stored_source (class_id UInt32, ngram String, count UInt64) ENGINE = MergeTree ORDER BY (class_id, ngram);
INSERT INTO nb_stored_source VALUES (0, 'alpha', 3), (0, 'beta', 2), (1, 'gamma', 4);

CREATE DICTIONARY nb_stored_model
(
    ngram String,
    class_id UInt32 DEFAULT 0,
    count UInt64 DEFAULT 0
)
PRIMARY KEY ngram
SOURCE(CLICKHOUSE(TABLE 'nb_stored_source'))
LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token' store_source 1))
LIFETIME(0);

SELECT 'Read training rows';
SELECT ngram, class_id, count FROM nb_stored_model ORDER BY ngram;
SELECT 'Still classifies';
SELECT naiveBayesClassifier('nb_stored_model', 'alpha beta');

DROP DICTIONARY IF EXISTS nb_stored_model;
DROP TABLE IF EXISTS nb_stored_source;

-- store_source reads columns back correctly when class_attribute names a non-first attribute.
DROP DICTIONARY IF EXISTS nb_stored_reordered;
DROP TABLE IF EXISTS nb_stored_reordered_source;

CREATE TABLE nb_stored_reordered_source (class_id UInt32, ngram String, count UInt64) ENGINE = MergeTree ORDER BY (class_id, ngram);
INSERT INTO nb_stored_reordered_source VALUES (0, 'alpha', 3), (1, 'gamma', 4);

CREATE DICTIONARY nb_stored_reordered
(
    ngram String,
    count UInt64 DEFAULT 0,
    class_id UInt32 DEFAULT 0
)
PRIMARY KEY ngram
SOURCE(CLICKHOUSE(TABLE 'nb_stored_reordered_source'))
LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token' store_source 1))
LIFETIME(0);

SELECT 'Read training rows with class_id as the second attribute';
SELECT ngram, count, class_id FROM nb_stored_reordered ORDER BY ngram;

DROP DICTIONARY IF EXISTS nb_stored_reordered;
DROP TABLE IF EXISTS nb_stored_reordered_source;


-- The class attribute can be named anything; naiveBayesClassifier and dictGet using that exact name agree,
-- which is the documented equivalence.
DROP TABLE IF EXISTS nb_label_source;
DROP DICTIONARY IF EXISTS nb_label_model;
CREATE TABLE nb_label_source (label UInt32, ngram String, count UInt64) ENGINE = MergeTree ORDER BY (label, ngram);
INSERT INTO nb_label_source VALUES (0, 'good', 10), (0, 'great', 8), (1, 'bad', 10), (1, 'awful', 7);
CREATE DICTIONARY nb_label_model
(
    ngram String,
    label UInt32 DEFAULT 0,
    count UInt64 DEFAULT 0
)
PRIMARY KEY ngram
SOURCE(CLICKHOUSE(TABLE 'nb_label_source'))
LAYOUT(NAIVE_BAYES(class_attribute 'label' n 1 mode 'token'))
LIFETIME(0);

SELECT 'Function equals dictGet on the configured class attribute name';
SELECT naiveBayesClassifier('nb_label_model', 'good') = dictGet('nb_label_model', 'label', 'good');
SELECT naiveBayesClassifier('nb_label_model', 'bad') = dictGet('nb_label_model', 'label', 'bad');

DROP DICTIONARY nb_label_model;
DROP TABLE nb_label_source;


-- Rows with count zero record no observation, so they must not change the trained model: a model with extra
-- count=0 rows must behave identically to one without them, including an n-gram that has a positive count in
-- one class but a zero count in another (which stays in the vocabulary through its positive row).
DROP TABLE IF EXISTS nb_zero_base_source;
DROP TABLE IF EXISTS nb_zero_extra_source;
DROP DICTIONARY IF EXISTS nb_zero_base;
DROP DICTIONARY IF EXISTS nb_zero_extra;
CREATE TABLE nb_zero_base_source (ngram String, class_id UInt32, count UInt64) ENGINE = MergeTree ORDER BY (class_id, ngram);
CREATE TABLE nb_zero_extra_source (ngram String, class_id UInt32, count UInt64) ENGINE = MergeTree ORDER BY (class_id, ngram);
INSERT INTO nb_zero_base_source VALUES ('x', 1, 1), ('y', 0, 3), ('z', 1, 5);
INSERT INTO nb_zero_extra_source VALUES ('x', 1, 1), ('y', 0, 3), ('z', 1, 5), ('z', 0, 0), ('unused_a', 0, 0), ('unused_b', 1, 0);
CREATE DICTIONARY nb_zero_base (ngram String, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_zero_base_source')) LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token' priors_mode 'proportional')) LIFETIME(0);
CREATE DICTIONARY nb_zero_extra (ngram String, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_zero_extra_source')) LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token' priors_mode 'proportional')) LIFETIME(0);

SELECT 'Zero-count rows do not change the model';
SELECT naiveBayesClassifier('nb_zero_extra', 'x') = naiveBayesClassifier('nb_zero_base', 'x');
SELECT naiveBayesClassifierWithAllProbs('nb_zero_extra', 'x') = naiveBayesClassifierWithAllProbs('nb_zero_base', 'x');
SELECT naiveBayesClassifierWithAllProbs('nb_zero_extra', 'z') = naiveBayesClassifierWithAllProbs('nb_zero_base', 'z');

DROP DICTIONARY nb_zero_base;
DROP DICTIONARY nb_zero_extra;
DROP TABLE nb_zero_base_source;
DROP TABLE nb_zero_extra_source;


DROP DICTIONARY IF EXISTS nb_token_model;
DROP DICTIONARY IF EXISTS nb_byte_model;
DROP DICTIONARY IF EXISTS nb_codepoint_model;
DROP DICTIONARY IF EXISTS nb_priors_model;
DROP DICTIONARY IF EXISTS nb_prop_model;
DROP DICTIONARY IF EXISTS nb_alpha_model;
DROP TABLE IF EXISTS texts_to_classify;
DROP TABLE IF EXISTS nb_source;
DROP TABLE IF EXISTS nb_byte_source;
DROP TABLE IF EXISTS nb_cp_source;
