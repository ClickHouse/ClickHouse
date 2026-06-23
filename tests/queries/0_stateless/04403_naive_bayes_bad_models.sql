-- Validates how a NaiveBayes dictionary rejects malformed model definitions (key/attribute types,
-- unknown or misused layout parameters, bad structure, empty source) and accepts valid ones.
-- Layout validation runs when the dictionary is first used, so each bad case is triggered by a query.

DROP TABLE IF EXISTS nb_bad_src;
CREATE TABLE nb_bad_src (class_id UInt32, ngram String, count UInt64) ENGINE = MergeTree ORDER BY (class_id, ngram);
INSERT INTO nb_bad_src VALUES (0, 'good', 10), (0, 'great', 8), (1, 'bad', 10), (1, 'awful', 6);

-- ---------- Wrong key / attribute types ----------

DROP DICTIONARY IF EXISTS nb_bad;
CREATE DICTIONARY nb_bad (ngram UInt64, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_bad_src')) LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token')) LIFETIME(0);
SELECT dictGet('nb_bad', 'class_id', 'good'); -- { serverError BAD_ARGUMENTS }
DROP DICTIONARY nb_bad;

CREATE DICTIONARY nb_bad (ngram String, class_id String DEFAULT '', count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_bad_src')) LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token')) LIFETIME(0);
SELECT dictGet('nb_bad', 'class_id', 'good'); -- { serverError BAD_ARGUMENTS }
DROP DICTIONARY nb_bad;

CREATE DICTIONARY nb_bad (ngram String, class_id Int32 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_bad_src')) LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token')) LIFETIME(0);
SELECT dictGet('nb_bad', 'class_id', 'good'); -- { serverError BAD_ARGUMENTS }
DROP DICTIONARY nb_bad;

CREATE DICTIONARY nb_bad (ngram String, class_id Float64 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_bad_src')) LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token')) LIFETIME(0);
SELECT dictGet('nb_bad', 'class_id', 'good'); -- { serverError BAD_ARGUMENTS }
DROP DICTIONARY nb_bad;

CREATE DICTIONARY nb_bad (ngram String, class_id UInt32 DEFAULT 0, count String DEFAULT '')
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_bad_src')) LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token')) LIFETIME(0);
SELECT dictGet('nb_bad', 'class_id', 'good'); -- { serverError BAD_ARGUMENTS }
DROP DICTIONARY nb_bad;

CREATE DICTIONARY nb_bad (ngram String, class_id UInt32 DEFAULT 0, count Float64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_bad_src')) LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token')) LIFETIME(0);
SELECT dictGet('nb_bad', 'class_id', 'good'); -- { serverError BAD_ARGUMENTS }
DROP DICTIONARY nb_bad;

-- ---------- Wrong number of attributes (must be exactly two) ----------

CREATE DICTIONARY nb_bad (ngram String, class_id UInt32 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_bad_src')) LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token')) LIFETIME(0);
SELECT dictGet('nb_bad', 'class_id', 'good'); -- { serverError BAD_ARGUMENTS }
DROP DICTIONARY nb_bad;

CREATE DICTIONARY nb_bad (ngram String, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0, extra UInt32 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_bad_src')) LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token')) LIFETIME(0);
SELECT dictGet('nb_bad', 'class_id', 'good'); -- { serverError BAD_ARGUMENTS }
DROP DICTIONARY nb_bad;

-- ---------- Unknown / misused layout parameters ----------

CREATE DICTIONARY nb_bad (ngram String, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_bad_src')) LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token' bogus 5)) LIFETIME(0);
SELECT dictGet('nb_bad', 'class_id', 'good'); -- { serverError BAD_ARGUMENTS }
DROP DICTIONARY nb_bad;

CREATE DICTIONARY nb_bad (ngram String, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_bad_src')) LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token' priors_mod 'uniform')) LIFETIME(0);
SELECT dictGet('nb_bad', 'class_id', 'good'); -- { serverError BAD_ARGUMENTS }
DROP DICTIONARY nb_bad;

-- `priors` is only valid with priors_mode 'explicit' (the silent-footgun cases)

CREATE DICTIONARY nb_bad (ngram String, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_bad_src')) LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token' priors '0=0.5,1=0.5')) LIFETIME(0);
SELECT dictGet('nb_bad', 'class_id', 'good'); -- { serverError BAD_ARGUMENTS }
DROP DICTIONARY nb_bad;

CREATE DICTIONARY nb_bad (ngram String, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_bad_src')) LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token' priors_mode 'uniform' priors '0=0.5,1=0.5')) LIFETIME(0);
SELECT dictGet('nb_bad', 'class_id', 'good'); -- { serverError BAD_ARGUMENTS }
DROP DICTIONARY nb_bad;

CREATE DICTIONARY nb_bad (ngram String, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_bad_src')) LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token' priors_mode 'proportional' priors '0=0.5,1=0.5')) LIFETIME(0);
SELECT dictGet('nb_bad', 'class_id', 'good'); -- { serverError BAD_ARGUMENTS }
DROP DICTIONARY nb_bad;

-- ---------- Missing or wrong class_attribute ----------

-- class_attribute is required.
CREATE DICTIONARY nb_bad (ngram String, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_bad_src')) LAYOUT(NAIVE_BAYES(n 1 mode 'token')) LIFETIME(0);
SELECT dictGet('nb_bad', 'class_id', 'good'); -- { serverError BAD_ARGUMENTS }
DROP DICTIONARY nb_bad;

-- class_attribute must name one of the attributes.
CREATE DICTIONARY nb_bad (ngram String, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_bad_src')) LAYOUT(NAIVE_BAYES(class_attribute 'nonexistent' n 1 mode 'token')) LIFETIME(0);
SELECT dictGet('nb_bad', 'class_id', 'good'); -- { serverError BAD_ARGUMENTS }
DROP DICTIONARY nb_bad;

-- ---------- Empty source ----------

DROP TABLE IF EXISTS nb_empty_src;
CREATE TABLE nb_empty_src (class_id UInt32, ngram String, count UInt64) ENGINE = MergeTree ORDER BY (class_id, ngram);
CREATE DICTIONARY nb_bad (ngram String, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_empty_src')) LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token')) LIFETIME(0);
SELECT dictGet('nb_bad', 'class_id', 'good'); -- { serverError RECEIVED_EMPTY_DATA }
DROP DICTIONARY nb_bad;
DROP TABLE nb_empty_src;

-- ---------- All-zero counts (proportional priors cannot be computed) ----------

DROP TABLE IF EXISTS nb_zero_src;
CREATE TABLE nb_zero_src (class_id UInt32, ngram String, count UInt64) ENGINE = MergeTree ORDER BY (class_id, ngram);
INSERT INTO nb_zero_src VALUES (0, 'zero', 0), (1, 'one', 0);
CREATE DICTIONARY nb_bad (ngram String, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_zero_src')) LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token')) LIFETIME(0);
SELECT dictGet('nb_bad', 'class_id', 'zero'); -- { serverError BAD_ARGUMENTS }
DROP DICTIONARY nb_bad;
DROP TABLE nb_zero_src;

-- ---------- Count sum overflows 64 bits ----------

DROP TABLE IF EXISTS nb_ovf_src;
CREATE TABLE nb_ovf_src (class_id UInt32, ngram String, count UInt64) ENGINE = MergeTree ORDER BY (class_id, ngram);
INSERT INTO nb_ovf_src VALUES (0, 'zero', 18446744073709551615), (0, 'zero', 1), (1, 'one', 1);
CREATE DICTIONARY nb_bad (ngram String, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_ovf_src')) LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token')) LIFETIME(0);
SELECT dictGet('nb_bad', 'class_id', 'zero'); -- { serverError BAD_ARGUMENTS }
DROP DICTIONARY nb_bad;
DROP TABLE nb_ovf_src;

-- ---------- Configured n does not match the source n-grams ----------

-- nb_bad_src holds unigrams, so loading them as bigrams is rejected.
CREATE DICTIONARY nb_bad (ngram String, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_bad_src')) LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 2 mode 'token')) LIFETIME(0);
SELECT dictGet('nb_bad', 'class_id', 'good'); -- { serverError BAD_ARGUMENTS }
DROP DICTIONARY nb_bad;

-- A single malformed n-gram is rejected even when it appears only after the first thousand rows, because
-- every source row is validated. It is ordered last so a check that stopped after a bounded sample would
-- never see it.
CREATE DICTIONARY nb_bad (ngram String, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(QUERY '
  SELECT ngram, class_id, count FROM (
    SELECT concat(''v'', toString(number)) AS ngram, toUInt32(number % 2) AS class_id, toUInt64(1) AS count, number AS ord FROM numbers(1100)
    UNION ALL
    SELECT ''zz two'', toUInt32(1), toUInt64(7), toUInt64(100000)
  ) ORDER BY ord
')) LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token')) LIFETIME(0);
SELECT dictGet('nb_bad', 'class_id', 'v1'); -- { serverError BAD_ARGUMENTS }
DROP DICTIONARY nb_bad;

-- ---------- n-gram size out of range (does not fit in 32 bits) ----------

CREATE DICTIONARY nb_bad (ngram String, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_bad_src')) LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 4294967297 mode 'token')) LIFETIME(0);
SELECT dictGet('nb_bad', 'class_id', 'good'); -- { serverError BAD_ARGUMENTS }
DROP DICTIONARY nb_bad;

-- ---------- Valid models (baseline + alternative unsigned types) ----------

SELECT 'valid UInt32/UInt64';
CREATE DICTIONARY nb_ok (ngram String, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_bad_src')) LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token')) LIFETIME(0);
SELECT naiveBayesClassifier('nb_ok', 'good great'), naiveBayesClassifier('nb_ok', 'bad awful');
DROP DICTIONARY nb_ok;

SELECT 'valid UInt8/UInt16';
CREATE DICTIONARY nb_ok (ngram String, class_id UInt8 DEFAULT 0, count UInt16 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_bad_src')) LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token')) LIFETIME(0);
SELECT naiveBayesClassifier('nb_ok', 'good great'), naiveBayesClassifier('nb_ok', 'bad awful');
DROP DICTIONARY nb_ok;

SELECT 'valid UInt64/UInt64';
CREATE DICTIONARY nb_ok (ngram String, class_id UInt64 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_bad_src')) LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token')) LIFETIME(0);
SELECT naiveBayesClassifier('nb_ok', 'good great'), naiveBayesClassifier('nb_ok', 'bad awful');
DROP DICTIONARY nb_ok;

-- A model whose source n-grams are bigrams loads and classifies under n 2.
SELECT 'valid n=2 bigram model';
DROP TABLE IF EXISTS nb_bigram_src;
CREATE TABLE nb_bigram_src (class_id UInt32, ngram String, count UInt64) ENGINE = MergeTree ORDER BY (class_id, ngram);
INSERT INTO nb_bigram_src VALUES (0, 'very good', 10), (0, 'really great', 8), (1, 'very bad', 10), (1, 'really awful', 6);
CREATE DICTIONARY nb_ok (ngram String, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_bigram_src')) LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 2 mode 'token')) LIFETIME(0);
SELECT naiveBayesClassifier('nb_ok', 'very good really great'), naiveBayesClassifier('nb_ok', 'very bad really awful');
DROP DICTIONARY nb_ok;
DROP TABLE nb_bigram_src;

-- Neither the PRIMARY KEY nor the attributes need a particular position: the key is found by being the key
-- and the class by `class_attribute`, both by name. Here ngram (the key) is declared last and count before
-- class_id, yet the model still classifies correctly — a positional read would misplace every column.
SELECT 'class_attribute and PRIMARY KEY resolve regardless of column order';
CREATE DICTIONARY nb_ok (count UInt64 DEFAULT 0, class_id UInt32 DEFAULT 0, ngram String)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_bad_src')) LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token')) LIFETIME(0);
SELECT naiveBayesClassifier('nb_ok', 'good great'), naiveBayesClassifier('nb_ok', 'bad awful');
DROP DICTIONARY nb_ok;

DROP TABLE nb_bad_src;
