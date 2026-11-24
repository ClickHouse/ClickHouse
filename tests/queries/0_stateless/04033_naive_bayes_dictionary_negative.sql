-- Test NaiveBayes dictionary error handling

DROP TABLE IF EXISTS nb_err_source;
DROP TABLE IF EXISTS nb_empty_source;
DROP DICTIONARY IF EXISTS nb_flat_dict;

CREATE TABLE nb_err_source
(
    class_id UInt32,
    ngram String,
    count UInt64
) ENGINE = MergeTree ORDER BY (class_id, ngram);

INSERT INTO nb_err_source VALUES (0, 'hello', 5), (1, 'world', 3);

-- Missing required 'n' parameter (error on first query)

CREATE DICTIONARY nb_no_n
(
    ngram String,
    class_id UInt32 DEFAULT 0,
    count UInt64 DEFAULT 0
)
PRIMARY KEY ngram
SOURCE(CLICKHOUSE(TABLE 'nb_err_source'))
LAYOUT(NAIVE_BAYES(class_attribute 'class_id' mode 'token' alpha 1.0))
LIFETIME(0);

SELECT dictGet('nb_no_n', 'class_id', 'test'); -- { serverError BAD_ARGUMENTS }

DROP DICTIONARY IF EXISTS nb_no_n;

-- Missing required 'mode' parameter

CREATE DICTIONARY nb_no_mode
(
    ngram String,
    class_id UInt32 DEFAULT 0,
    count UInt64 DEFAULT 0
)
PRIMARY KEY ngram
SOURCE(CLICKHOUSE(TABLE 'nb_err_source'))
LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1))
LIFETIME(0);

SELECT dictGet('nb_no_mode', 'class_id', 'test'); -- { serverError BAD_ARGUMENTS }

DROP DICTIONARY IF EXISTS nb_no_mode;

-- Invalid mode

CREATE DICTIONARY nb_bad_mode
(
    ngram String,
    class_id UInt32 DEFAULT 0,
    count UInt64 DEFAULT 0
)
PRIMARY KEY ngram
SOURCE(CLICKHOUSE(TABLE 'nb_err_source'))
LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'invalid'))
LIFETIME(0);

SELECT dictGet('nb_bad_mode', 'class_id', 'test'); -- { serverError BAD_ARGUMENTS }

DROP DICTIONARY IF EXISTS nb_bad_mode;

-- Zero n-gram size

CREATE DICTIONARY nb_zero_n
(
    ngram String,
    class_id UInt32 DEFAULT 0,
    count UInt64 DEFAULT 0
)
PRIMARY KEY ngram
SOURCE(CLICKHOUSE(TABLE 'nb_err_source'))
LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 0 mode 'token'))
LIFETIME(0);

SELECT dictGet('nb_zero_n', 'class_id', 'test'); -- { serverError BAD_ARGUMENTS }

DROP DICTIONARY IF EXISTS nb_zero_n;

-- Non-positive alpha

CREATE DICTIONARY nb_bad_alpha
(
    ngram String,
    class_id UInt32 DEFAULT 0,
    count UInt64 DEFAULT 0
)
PRIMARY KEY ngram
SOURCE(CLICKHOUSE(TABLE 'nb_err_source'))
LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token' alpha 0))
LIFETIME(0);

SELECT dictGet('nb_bad_alpha', 'class_id', 'test'); -- { serverError BAD_ARGUMENTS }

DROP DICTIONARY IF EXISTS nb_bad_alpha;

-- alpha so large that the smoothing term (alpha * vocabulary size) overflows to infinity

CREATE DICTIONARY nb_huge_alpha
(
    ngram String,
    class_id UInt32 DEFAULT 0,
    count UInt64 DEFAULT 0
)
PRIMARY KEY ngram
SOURCE(CLICKHOUSE(TABLE 'nb_err_source'))
LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token' alpha 1e308))
LIFETIME(0);

SELECT dictGet('nb_huge_alpha', 'class_id', 'test'); -- { serverError BAD_ARGUMENTS }

DROP DICTIONARY IF EXISTS nb_huge_alpha;

-- Priors don't sum to 1.0

CREATE DICTIONARY nb_bad_priors_sum
(
    ngram String,
    class_id UInt32 DEFAULT 0,
    count UInt64 DEFAULT 0
)
PRIMARY KEY ngram
SOURCE(CLICKHOUSE(TABLE 'nb_err_source'))
LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token' priors_mode 'explicit' priors [(0, 0.5), (1, 0.3)]))
LIFETIME(0);

SELECT dictGet('nb_bad_priors_sum', 'class_id', 'test'); -- { serverError BAD_ARGUMENTS }

DROP DICTIONARY IF EXISTS nb_bad_priors_sum;

-- Priors with prob <= 0

CREATE DICTIONARY nb_neg_prior
(
    ngram String,
    class_id UInt32 DEFAULT 0,
    count UInt64 DEFAULT 0
)
PRIMARY KEY ngram
SOURCE(CLICKHOUSE(TABLE 'nb_err_source'))
LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token' priors_mode 'explicit' priors [(0, -0.5), (1, 1.5)]))
LIFETIME(0);

SELECT dictGet('nb_neg_prior', 'class_id', 'test'); -- { serverError BAD_ARGUMENTS }

DROP DICTIONARY IF EXISTS nb_neg_prior;

-- Priors with prob > 1

CREATE DICTIONARY nb_big_prior
(
    ngram String,
    class_id UInt32 DEFAULT 0,
    count UInt64 DEFAULT 0
)
PRIMARY KEY ngram
SOURCE(CLICKHOUSE(TABLE 'nb_err_source'))
LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token' priors_mode 'explicit' priors [(0, 2.0), (1, 0.5)]))
LIFETIME(0);

SELECT dictGet('nb_big_prior', 'class_id', 'test'); -- { serverError BAD_ARGUMENTS }

DROP DICTIONARY IF EXISTS nb_big_prior;

-- Priors with a non-finite probability (NaN or infinity)

CREATE DICTIONARY nb_nan_prior
(
    ngram String,
    class_id UInt32 DEFAULT 0,
    count UInt64 DEFAULT 0
)
PRIMARY KEY ngram
SOURCE(CLICKHOUSE(TABLE 'nb_err_source'))
LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token' priors_mode 'explicit' priors [(0, 'nan'), (1, 0.5)]))
LIFETIME(0);

SELECT dictGet('nb_nan_prior', 'class_id', 'test'); -- { serverError BAD_ARGUMENTS }

DROP DICTIONARY IF EXISTS nb_nan_prior;

CREATE DICTIONARY nb_inf_prior
(
    ngram String,
    class_id UInt32 DEFAULT 0,
    count UInt64 DEFAULT 0
)
PRIMARY KEY ngram
SOURCE(CLICKHOUSE(TABLE 'nb_err_source'))
LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token' priors_mode 'explicit' priors [(0, 'inf'), (1, 0.5)]))
LIFETIME(0);

SELECT dictGet('nb_inf_prior', 'class_id', 'test'); -- { serverError BAD_ARGUMENTS }

DROP DICTIONARY IF EXISTS nb_inf_prior;

-- Priors whose probability is itself a collection instead of a scalar

CREATE DICTIONARY nb_nested_prior
(
    ngram String,
    class_id UInt32 DEFAULT 0,
    count UInt64 DEFAULT 0
)
PRIMARY KEY ngram
SOURCE(CLICKHOUSE(TABLE 'nb_err_source'))
LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token' priors_mode 'explicit' priors [(0, (0.5, 0.5)), (1, 0.5)]))
LIFETIME(0); -- { serverError BAD_ARGUMENTS }

DROP DICTIONARY IF EXISTS nb_nested_prior;

-- A priors entry with more than two elements is not a (class, probability) pair

CREATE DICTIONARY nb_wide_prior
(
    ngram String,
    class_id UInt32 DEFAULT 0,
    count UInt64 DEFAULT 0
)
PRIMARY KEY ngram
SOURCE(CLICKHOUSE(TABLE 'nb_err_source'))
LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token' priors_mode 'explicit' priors [(0, 0.5, 1), (1, 0.5)]))
LIFETIME(0); -- { serverError BAD_ARGUMENTS }

DROP DICTIONARY IF EXISTS nb_wide_prior;

-- Priors class count mismatch (3 priors for 2 classes)

CREATE DICTIONARY nb_prior_count_mismatch
(
    ngram String,
    class_id UInt32 DEFAULT 0,
    count UInt64 DEFAULT 0
)
PRIMARY KEY ngram
SOURCE(CLICKHOUSE(TABLE 'nb_err_source'))
LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token' priors_mode 'explicit' priors [(0, 0.4), (1, 0.3), (2, 0.3)]))
LIFETIME(0);

SELECT dictGet('nb_prior_count_mismatch', 'class_id', 'test'); -- { serverError BAD_ARGUMENTS }

DROP DICTIONARY IF EXISTS nb_prior_count_mismatch;

-- Priors with class not in model

CREATE DICTIONARY nb_prior_unknown_class
(
    ngram String,
    class_id UInt32 DEFAULT 0,
    count UInt64 DEFAULT 0
)
PRIMARY KEY ngram
SOURCE(CLICKHOUSE(TABLE 'nb_err_source'))
LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token' priors_mode 'explicit' priors [(0, 0.5), (5, 0.5)]))
LIFETIME(0);

SELECT dictGet('nb_prior_unknown_class', 'class_id', 'test'); -- { serverError BAD_ARGUMENTS }

DROP DICTIONARY IF EXISTS nb_prior_unknown_class;

-- Empty source table

CREATE TABLE nb_empty_source
(
    class_id UInt32,
    ngram String,
    count UInt64
) ENGINE = MergeTree ORDER BY (class_id, ngram);

CREATE DICTIONARY nb_empty_model
(
    ngram String,
    class_id UInt32 DEFAULT 0,
    count UInt64 DEFAULT 0
)
PRIMARY KEY ngram
SOURCE(CLICKHOUSE(TABLE 'nb_empty_source'))
LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token'))
LIFETIME(0);

SELECT dictGet('nb_empty_model', 'class_id', 'test'); -- { serverError RECEIVED_EMPTY_DATA }

DROP DICTIONARY IF EXISTS nb_empty_model;

-- A valid NaiveBayes dictionary, reused by the argument-type and attribute negative cases below.

CREATE DICTIONARY nb_err_dict
(
    ngram String,
    class_id UInt32 DEFAULT 0,
    count UInt64 DEFAULT 0
)
PRIMARY KEY ngram
SOURCE(CLICKHOUSE(TABLE 'nb_err_source'))
LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token'))
LIFETIME(0);

-- Wrong argument types — all three functions

SELECT naiveBayesClassifier(123, 'hello');               -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT naiveBayesClassifier('nb_err_dict', 123);         -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT naiveBayesClassifierWithProb(123, 'hello');       -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT naiveBayesClassifierWithProb('nb_err_dict', 123); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT naiveBayesClassifierWithAllProbs(123, 'hello');       -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT naiveBayesClassifierWithAllProbs('nb_err_dict', 123); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Non-constant dictionary name is rejected — all three functions

SELECT naiveBayesClassifier(materialize('nb_err_dict'), 'hello');          -- { serverError ILLEGAL_COLUMN }
SELECT naiveBayesClassifierWithProb(materialize('nb_err_dict'), 'hello');  -- { serverError ILLEGAL_COLUMN }
SELECT naiveBayesClassifierWithAllProbs(materialize('nb_err_dict'), 'hello');  -- { serverError ILLEGAL_COLUMN }

-- Non-NB dictionary passed to naiveBayesClassifier

DROP DICTIONARY IF EXISTS nb_flat_dict;

CREATE TABLE IF NOT EXISTS nb_flat_source (id UInt64, val String) ENGINE = MergeTree ORDER BY id;
INSERT INTO nb_flat_source VALUES (1, 'a');

CREATE DICTIONARY nb_flat_dict (id UInt64, val String DEFAULT '')
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'nb_flat_source'))
LAYOUT(FLAT())
LIFETIME(0);

SELECT naiveBayesClassifier('nb_flat_dict', 'hello');          -- { serverError BAD_ARGUMENTS }
SELECT naiveBayesClassifierWithProb('nb_flat_dict', 'hello');  -- { serverError BAD_ARGUMENTS }
SELECT naiveBayesClassifierWithAllProbs('nb_flat_dict', 'hello');  -- { serverError BAD_ARGUMENTS }

DROP DICTIONARY IF EXISTS nb_flat_dict;
DROP TABLE IF EXISTS nb_flat_source;

-- Wrong attribute name passed to dictGet

SELECT dictGet('nb_err_dict', 'count', 'test'); -- { serverError UNSUPPORTED_METHOD }

-- Uniform priors_mode (covers the uniform branch)

DROP DICTIONARY IF EXISTS nb_uniform_explicit;

CREATE DICTIONARY nb_uniform_explicit
(
    ngram String,
    class_id UInt32 DEFAULT 0,
    count UInt64 DEFAULT 0
)
PRIMARY KEY ngram
SOURCE(CLICKHOUSE(TABLE 'nb_err_source'))
LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token' priors_mode 'uniform'))
LIFETIME(0);

SELECT 'Uniform explicit';
SELECT naiveBayesClassifier('nb_uniform_explicit', 'hello');

DROP DICTIONARY IF EXISTS nb_uniform_explicit;

-- Explicit priors_mode without a priors parameter

CREATE DICTIONARY nb_explicit_no_priors
(
    ngram String,
    class_id UInt32 DEFAULT 0,
    count UInt64 DEFAULT 0
)
PRIMARY KEY ngram
SOURCE(CLICKHOUSE(TABLE 'nb_err_source'))
LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token' priors_mode 'explicit'))
LIFETIME(0);

SELECT dictGet('nb_explicit_no_priors', 'class_id', 'test'); -- { serverError BAD_ARGUMENTS }

DROP DICTIONARY IF EXISTS nb_explicit_no_priors;

-- Invalid priors_mode value

CREATE DICTIONARY nb_bad_priors_mode
(
    ngram String,
    class_id UInt32 DEFAULT 0,
    count UInt64 DEFAULT 0
)
PRIMARY KEY ngram
SOURCE(CLICKHOUSE(TABLE 'nb_err_source'))
LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token' priors_mode 'bogus'))
LIFETIME(0);

SELECT dictGet('nb_bad_priors_mode', 'class_id', 'test'); -- { serverError BAD_ARGUMENTS }

DROP DICTIONARY IF EXISTS nb_bad_priors_mode;

-- Explicit priors with a non-numeric probability

CREATE DICTIONARY nb_nonnumeric_prior
(
    ngram String,
    class_id UInt32 DEFAULT 0,
    count UInt64 DEFAULT 0
)
PRIMARY KEY ngram
SOURCE(CLICKHOUSE(TABLE 'nb_err_source'))
LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token' priors_mode 'explicit' priors '0=abc,1=0.5'))
LIFETIME(0);

SELECT dictGet('nb_nonnumeric_prior', 'class_id', 'test'); -- { serverError BAD_ARGUMENTS }

DROP DICTIONARY IF EXISTS nb_nonnumeric_prior;

-- Reading from the dictionary without store_source

SELECT * FROM nb_err_dict; -- { serverError UNSUPPORTED_METHOD }

-- A map literal is not valid priors syntax: priors are written as an array of (class, probability) pairs.
-- The whole statement stays on one line: on a syntax error the client searches for the expected-error
-- hint only until the end of the line where parsing failed.

CREATE DICTIONARY nb_map_prior (ngram String, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0) PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_err_source')) LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token' priors_mode 'explicit' priors {0: 0.9, 1: 0.1})) LIFETIME(0); -- { clientError SYNTAX_ERROR }

DROP DICTIONARY IF EXISTS nb_map_prior;

-- Cleanup (dictionaries first, then tables)

DROP DICTIONARY IF EXISTS nb_err_dict;
DROP TABLE IF EXISTS nb_err_source;
DROP TABLE IF EXISTS nb_empty_source;
