-- Validates how a NaiveBayes dictionary rejects malformed model definitions (key/attribute types,
-- unknown or misused layout parameters, bad structure, empty source) and accepts valid ones.
-- Layout validation runs when the dictionary is first used, so each bad case is triggered by a query.

DROP TABLE IF EXISTS nb_bad_src;
CREATE TABLE nb_bad_src (class_id UInt32, ngram String, count UInt64) ENGINE = MergeTree ORDER BY (class_id, ngram);
INSERT INTO nb_bad_src VALUES (0, 'good', 10), (0, 'great', 8), (1, 'bad', 10), (1, 'awful', 6);

-- ---------- Wrong key / attribute types ----------

DROP DICTIONARY IF EXISTS nb_bad;
CREATE DICTIONARY nb_bad (ngram UInt64, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_bad_src')) LAYOUT(NAIVE_BAYES(n 1 mode 'token')) LIFETIME(0);
SELECT dictGet('nb_bad', 'class_id', 'good'); -- { serverError BAD_ARGUMENTS }
DROP DICTIONARY nb_bad;

CREATE DICTIONARY nb_bad (ngram String, class_id String DEFAULT '', count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_bad_src')) LAYOUT(NAIVE_BAYES(n 1 mode 'token')) LIFETIME(0);
SELECT dictGet('nb_bad', 'class_id', 'good'); -- { serverError BAD_ARGUMENTS }
DROP DICTIONARY nb_bad;

CREATE DICTIONARY nb_bad (ngram String, class_id Int32 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_bad_src')) LAYOUT(NAIVE_BAYES(n 1 mode 'token')) LIFETIME(0);
SELECT dictGet('nb_bad', 'class_id', 'good'); -- { serverError BAD_ARGUMENTS }
DROP DICTIONARY nb_bad;

CREATE DICTIONARY nb_bad (ngram String, class_id Float64 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_bad_src')) LAYOUT(NAIVE_BAYES(n 1 mode 'token')) LIFETIME(0);
SELECT dictGet('nb_bad', 'class_id', 'good'); -- { serverError BAD_ARGUMENTS }
DROP DICTIONARY nb_bad;

CREATE DICTIONARY nb_bad (ngram String, class_id UInt32 DEFAULT 0, count String DEFAULT '')
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_bad_src')) LAYOUT(NAIVE_BAYES(n 1 mode 'token')) LIFETIME(0);
SELECT dictGet('nb_bad', 'class_id', 'good'); -- { serverError BAD_ARGUMENTS }
DROP DICTIONARY nb_bad;

CREATE DICTIONARY nb_bad (ngram String, class_id UInt32 DEFAULT 0, count Float64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_bad_src')) LAYOUT(NAIVE_BAYES(n 1 mode 'token')) LIFETIME(0);
SELECT dictGet('nb_bad', 'class_id', 'good'); -- { serverError BAD_ARGUMENTS }
DROP DICTIONARY nb_bad;

-- ---------- Fewer than two attributes ----------

CREATE DICTIONARY nb_bad (ngram String, class_id UInt32 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_bad_src')) LAYOUT(NAIVE_BAYES(n 1 mode 'token')) LIFETIME(0);
SELECT dictGet('nb_bad', 'class_id', 'good'); -- { serverError BAD_ARGUMENTS }
DROP DICTIONARY nb_bad;

-- ---------- Unknown / misused layout parameters ----------

CREATE DICTIONARY nb_bad (ngram String, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_bad_src')) LAYOUT(NAIVE_BAYES(n 1 mode 'token' bogus 5)) LIFETIME(0);
SELECT dictGet('nb_bad', 'class_id', 'good'); -- { serverError BAD_ARGUMENTS }
DROP DICTIONARY nb_bad;

CREATE DICTIONARY nb_bad (ngram String, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_bad_src')) LAYOUT(NAIVE_BAYES(n 1 mode 'token' priors_mod 'uniform')) LIFETIME(0);
SELECT dictGet('nb_bad', 'class_id', 'good'); -- { serverError BAD_ARGUMENTS }
DROP DICTIONARY nb_bad;

-- `priors` is only valid with priors_mode 'explicit' (the silent-footgun cases)

CREATE DICTIONARY nb_bad (ngram String, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_bad_src')) LAYOUT(NAIVE_BAYES(n 1 mode 'token' priors '0=0.5,1=0.5')) LIFETIME(0);
SELECT dictGet('nb_bad', 'class_id', 'good'); -- { serverError BAD_ARGUMENTS }
DROP DICTIONARY nb_bad;

CREATE DICTIONARY nb_bad (ngram String, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_bad_src')) LAYOUT(NAIVE_BAYES(n 1 mode 'token' priors_mode 'uniform' priors '0=0.5,1=0.5')) LIFETIME(0);
SELECT dictGet('nb_bad', 'class_id', 'good'); -- { serverError BAD_ARGUMENTS }
DROP DICTIONARY nb_bad;

CREATE DICTIONARY nb_bad (ngram String, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_bad_src')) LAYOUT(NAIVE_BAYES(n 1 mode 'token' priors_mode 'proportional' priors '0=0.5,1=0.5')) LIFETIME(0);
SELECT dictGet('nb_bad', 'class_id', 'good'); -- { serverError BAD_ARGUMENTS }
DROP DICTIONARY nb_bad;

-- ---------- Empty source ----------

DROP TABLE IF EXISTS nb_empty_src;
CREATE TABLE nb_empty_src (class_id UInt32, ngram String, count UInt64) ENGINE = MergeTree ORDER BY (class_id, ngram);
CREATE DICTIONARY nb_bad (ngram String, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_empty_src')) LAYOUT(NAIVE_BAYES(n 1 mode 'token')) LIFETIME(0);
SELECT dictGet('nb_bad', 'class_id', 'good'); -- { serverError RECEIVED_EMPTY_DATA }
DROP DICTIONARY nb_bad;
DROP TABLE nb_empty_src;

-- ---------- Valid models (baseline + alternative unsigned types) ----------

SELECT 'valid UInt32/UInt64';
CREATE DICTIONARY nb_ok (ngram String, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_bad_src')) LAYOUT(NAIVE_BAYES(n 1 mode 'token')) LIFETIME(0);
SELECT naiveBayesClassifier('nb_ok', 'good great'), naiveBayesClassifier('nb_ok', 'bad awful');
DROP DICTIONARY nb_ok;

SELECT 'valid UInt8/UInt16';
CREATE DICTIONARY nb_ok (ngram String, class_id UInt8 DEFAULT 0, count UInt16 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_bad_src')) LAYOUT(NAIVE_BAYES(n 1 mode 'token')) LIFETIME(0);
SELECT naiveBayesClassifier('nb_ok', 'good great'), naiveBayesClassifier('nb_ok', 'bad awful');
DROP DICTIONARY nb_ok;

SELECT 'valid UInt64/UInt64';
CREATE DICTIONARY nb_ok (ngram String, class_id UInt64 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_bad_src')) LAYOUT(NAIVE_BAYES(n 1 mode 'token')) LIFETIME(0);
SELECT naiveBayesClassifier('nb_ok', 'good great'), naiveBayesClassifier('nb_ok', 'bad awful');
DROP DICTIONARY nb_ok;

DROP TABLE nb_bad_src;
