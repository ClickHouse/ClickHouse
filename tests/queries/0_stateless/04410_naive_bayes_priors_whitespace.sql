-- Explicit priors tolerate whitespace around the class ids, the '=' separators, and the comma-separated
-- entries: each field is trimmed before parsing. Every whitespace variant below must load and produce a
-- model identical to the canonical 'class=prob,...' form, so every comparison prints 1. Whitespace inside a
-- single value (a split class id or probability) is malformed and is covered by 04403 instead.

DROP TABLE IF EXISTS nb_ws_source;
CREATE TABLE nb_ws_source (class_id UInt32, ngram String, count UInt64) ENGINE = MergeTree ORDER BY (class_id, ngram);
INSERT INTO nb_ws_source VALUES (0, 'good', 10), (0, 'great', 8), (1, 'bad', 10), (1, 'awful', 7);

-- The canonical, space-free baseline that every variant is compared against.
DROP DICTIONARY IF EXISTS nb_ws_base;
CREATE DICTIONARY nb_ws_base (ngram String, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_ws_source'))
LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token' priors_mode 'explicit' priors '0=0.9,1=0.1')) LIFETIME(0);

-- Each variant is compared to the baseline over two inputs; equal probabilities prove the parsed priors
-- (and therefore the whole model) are identical regardless of the surrounding whitespace.
CREATE DICTIONARY nb_ws_var (ngram String, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_ws_source'))
LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token' priors_mode 'explicit' priors '0 = 0.9 , 1 = 0.1')) LIFETIME(0);
SELECT naiveBayesClassifierAllProbs('nb_ws_var', 'good') = naiveBayesClassifierAllProbs('nb_ws_base', 'good')
   AND naiveBayesClassifierAllProbs('nb_ws_var', 'bad')  = naiveBayesClassifierAllProbs('nb_ws_base', 'bad'); -- spaces around '=' and ','
DROP DICTIONARY nb_ws_var;

CREATE DICTIONARY nb_ws_var (ngram String, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_ws_source'))
LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token' priors_mode 'explicit' priors '0=0.9, 1=0.1')) LIFETIME(0);
SELECT naiveBayesClassifierAllProbs('nb_ws_var', 'good') = naiveBayesClassifierAllProbs('nb_ws_base', 'good')
   AND naiveBayesClassifierAllProbs('nb_ws_var', 'bad')  = naiveBayesClassifierAllProbs('nb_ws_base', 'bad'); -- space after ','
DROP DICTIONARY nb_ws_var;

CREATE DICTIONARY nb_ws_var (ngram String, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_ws_source'))
LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token' priors_mode 'explicit' priors '  0=0.9,1=0.1  ')) LIFETIME(0);
SELECT naiveBayesClassifierAllProbs('nb_ws_var', 'good') = naiveBayesClassifierAllProbs('nb_ws_base', 'good')
   AND naiveBayesClassifierAllProbs('nb_ws_var', 'bad')  = naiveBayesClassifierAllProbs('nb_ws_base', 'bad'); -- leading/trailing spaces
DROP DICTIONARY nb_ws_var;

CREATE DICTIONARY nb_ws_var (ngram String, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_ws_source'))
LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token' priors_mode 'explicit' priors '0=0.9 ,1=0.1')) LIFETIME(0);
SELECT naiveBayesClassifierAllProbs('nb_ws_var', 'good') = naiveBayesClassifierAllProbs('nb_ws_base', 'good')
   AND naiveBayesClassifierAllProbs('nb_ws_var', 'bad')  = naiveBayesClassifierAllProbs('nb_ws_base', 'bad'); -- space before ','
DROP DICTIONARY nb_ws_var;

CREATE DICTIONARY nb_ws_var (ngram String, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_ws_source'))
LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token' priors_mode 'explicit' priors '0=0.9,   1=0.1')) LIFETIME(0);
SELECT naiveBayesClassifierAllProbs('nb_ws_var', 'good') = naiveBayesClassifierAllProbs('nb_ws_base', 'good')
   AND naiveBayesClassifierAllProbs('nb_ws_var', 'bad')  = naiveBayesClassifierAllProbs('nb_ws_base', 'bad'); -- multiple spaces after ','
DROP DICTIONARY nb_ws_var;

CREATE DICTIONARY nb_ws_var (ngram String, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_ws_source'))
LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token' priors_mode 'explicit' priors '0\t=\t0.9,1\t=\t0.1')) LIFETIME(0);
SELECT naiveBayesClassifierAllProbs('nb_ws_var', 'good') = naiveBayesClassifierAllProbs('nb_ws_base', 'good')
   AND naiveBayesClassifierAllProbs('nb_ws_var', 'bad')  = naiveBayesClassifierAllProbs('nb_ws_base', 'bad'); -- tabs around '='
DROP DICTIONARY nb_ws_var;

CREATE DICTIONARY nb_ws_var (ngram String, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_ws_source'))
LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token' priors_mode 'explicit' priors ' 0 = 0.9 , 1 = 0.1 ')) LIFETIME(0);
SELECT naiveBayesClassifierAllProbs('nb_ws_var', 'good') = naiveBayesClassifierAllProbs('nb_ws_base', 'good')
   AND naiveBayesClassifierAllProbs('nb_ws_var', 'bad')  = naiveBayesClassifierAllProbs('nb_ws_base', 'bad'); -- whitespace everywhere
DROP DICTIONARY nb_ws_var;

DROP DICTIONARY nb_ws_base;
DROP TABLE nb_ws_source;
