-- Test the token mode of the NaiveBayes dictionary on a small sentiment model

DROP TABLE IF EXISTS nb_sentiment_data;
DROP DICTIONARY IF EXISTS sentiment_token_1;

CREATE TABLE nb_sentiment_data (class_id UInt32, ngram String, count UInt64) ENGINE = MergeTree ORDER BY (class_id, ngram);

INSERT INTO nb_sentiment_data VALUES
    (0, 'beautiful', 5), (0, 'intuitive', 3), (0, 'amazing', 8), (0, 'impressed', 4), (0, 'excellent', 7),
    (0, 'quality', 5), (0, 'wonderful', 3), (0, 'great', 6), (0, 'love', 4), (0, 'perfect', 3),
    (0, 'fantastic', 2), (0, 'best', 3), (0, 'interface', 2), (0, 'product', 1),
    (1, 'awful', 8), (1, 'horrible', 6), (1, 'disaster', 5), (1, 'terrible', 7), (1, 'worst', 4),
    (1, 'barely', 3), (1, 'poor', 3), (1, 'disappointing', 2), (1, 'broken', 2), (1, 'useless', 2),
    (1, 'support', 1), (1, 'app', 1);

CREATE DICTIONARY sentiment_token_1
(
    ngram String,
    class_id UInt32 DEFAULT 0,
    count UInt64 DEFAULT 0
)
PRIMARY KEY ngram
SOURCE(CLICKHOUSE(TABLE 'nb_sentiment_data'))
LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token' alpha 1.0))
LIFETIME(0);

SELECT naiveBayesClassifier('sentiment_token_1', 'The interface is beautiful and intuitive');
SELECT naiveBayesClassifier('sentiment_token_1', 'This product is amazing in every way');
SELECT naiveBayesClassifier('sentiment_token_1', 'I am impressed by the excellent quality');
SELECT naiveBayesClassifier('sentiment_token_1', 'The app is awful and barely works');
SELECT naiveBayesClassifier('sentiment_token_1', 'Customer support was horrible today');
SELECT naiveBayesClassifier('sentiment_token_1', 'This experience was a total disaster');

SELECT dictGet('sentiment_token_1', 'class_id', 'The interface is beautiful and intuitive');
SELECT dictGet('sentiment_token_1', 'class_id', 'The app is awful and barely works');

SELECT (w.1, round(w.2, 4)) FROM (SELECT naiveBayesClassifierWithProb('sentiment_token_1', 'The interface is beautiful and intuitive') AS w);
SELECT (w.1, round(w.2, 4)) FROM (SELECT naiveBayesClassifierWithProb('sentiment_token_1', 'The app is awful and barely works') AS w);

SELECT arrayMap(p -> (p.1, round(p.2, 4)), naiveBayesClassifierWithAllProbs('sentiment_token_1', 'The interface is beautiful and intuitive'));
SELECT arrayMap(p -> (p.1, round(p.2, 4)), naiveBayesClassifierWithAllProbs('sentiment_token_1', 'The app is awful and barely works'));

DROP DICTIONARY IF EXISTS sentiment_token_1;
DROP TABLE IF EXISTS nb_sentiment_data;

-- A token n-gram is normalized to the form the tokenizer emits at query time: its words joined by single
-- spaces. Source n-grams that differ only in leading, trailing, or repeated whitespace (or tabs) fold onto
-- that key and keep their counts, instead of being stored under a key no query can reach. A model built from
-- such variants must behave exactly like one built from the canonical rows with those counts summed.

-- n = 1: a trailing space on the source n-gram must not change the trained model.
DROP TABLE IF EXISTS nb_ws1_variant;
DROP TABLE IF EXISTS nb_ws1_canonical;
DROP DICTIONARY IF EXISTS nb_ws1_variant_dict;
DROP DICTIONARY IF EXISTS nb_ws1_canonical_dict;
CREATE TABLE nb_ws1_variant (class_id UInt32, ngram String, count UInt64) ENGINE = MergeTree ORDER BY (class_id, ngram);
CREATE TABLE nb_ws1_canonical (class_id UInt32, ngram String, count UInt64) ENGINE = MergeTree ORDER BY (class_id, ngram);
INSERT INTO nb_ws1_variant VALUES (0, 'good ', 1000), (1, 'bad', 1);
INSERT INTO nb_ws1_canonical VALUES (0, 'good', 1000), (1, 'bad', 1);
CREATE DICTIONARY nb_ws1_variant_dict (ngram String, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_ws1_variant')) LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token')) LIFETIME(0);
CREATE DICTIONARY nb_ws1_canonical_dict (ngram String, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_ws1_canonical')) LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token')) LIFETIME(0);
SELECT naiveBayesClassifierWithAllProbs('nb_ws1_variant_dict', 'good') = naiveBayesClassifierWithAllProbs('nb_ws1_canonical_dict', 'good');
SELECT naiveBayesClassifier('nb_ws1_variant_dict', 'good') = naiveBayesClassifier('nb_ws1_canonical_dict', 'good');
DROP DICTIONARY nb_ws1_variant_dict;
DROP DICTIONARY nb_ws1_canonical_dict;
DROP TABLE nb_ws1_variant;
DROP TABLE nb_ws1_canonical;

-- n = 2: leading, trailing, repeated, and tab whitespace all fold onto the single-space key.
DROP TABLE IF EXISTS nb_ws2_variant;
DROP TABLE IF EXISTS nb_ws2_canonical;
DROP DICTIONARY IF EXISTS nb_ws2_variant_dict;
DROP DICTIONARY IF EXISTS nb_ws2_canonical_dict;
CREATE TABLE nb_ws2_variant (class_id UInt32, ngram String, count UInt64) ENGINE = MergeTree ORDER BY (class_id, ngram);
CREATE TABLE nb_ws2_canonical (class_id UInt32, ngram String, count UInt64) ENGINE = MergeTree ORDER BY (class_id, ngram);
INSERT INTO nb_ws2_variant VALUES
    (0, 'good word', 500), (0, 'good  word', 300), (0, '  good word  ', 100), (0, 'good\tword', 100),
    (1, 'bad word', 5);
INSERT INTO nb_ws2_canonical VALUES (0, 'good word', 1000), (1, 'bad word', 5);
CREATE DICTIONARY nb_ws2_variant_dict (ngram String, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_ws2_variant')) LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 2 mode 'token')) LIFETIME(0);
CREATE DICTIONARY nb_ws2_canonical_dict (ngram String, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_ws2_canonical')) LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 2 mode 'token')) LIFETIME(0);
SELECT naiveBayesClassifierWithAllProbs('nb_ws2_variant_dict', 'good word') = naiveBayesClassifierWithAllProbs('nb_ws2_canonical_dict', 'good word');
SELECT element_count = 2 FROM system.dictionaries WHERE database = currentDatabase() AND name = 'nb_ws2_variant_dict';
DROP DICTIONARY nb_ws2_variant_dict;
DROP DICTIONARY nb_ws2_canonical_dict;
DROP TABLE nb_ws2_variant;
DROP TABLE nb_ws2_canonical;
