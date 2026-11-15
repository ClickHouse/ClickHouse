-- Validates how a NaiveBayes dictionary rejects malformed model definitions (key/attribute types,
-- unknown or misused layout parameters, bad structure, empty source) and accepts valid ones.
-- Layout validation runs when the dictionary is first used, so most bad cases are triggered by a
-- query; a priors collection of the wrong shape is rejected already when the dictionary is created.

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

-- ---------- Wrong number of key columns (must be exactly one) ----------

DROP TABLE IF EXISTS nb_two_key_src;
CREATE TABLE nb_two_key_src (a String, b String, class_id UInt32, count UInt64) ENGINE = MergeTree ORDER BY (a, b);
CREATE DICTIONARY nb_bad (a String, b String, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY a, b SOURCE(CLICKHOUSE(TABLE 'nb_two_key_src')) LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token')) LIFETIME(0);
SELECT dictGet('nb_bad', 'class_id', 'x'); -- { serverError BAD_ARGUMENTS }
DROP DICTIONARY nb_bad;
DROP TABLE nb_two_key_src;

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
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_bad_src')) LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token' priors [(0, 0.5), (1, 0.5)])) LIFETIME(0);
SELECT dictGet('nb_bad', 'class_id', 'good'); -- { serverError BAD_ARGUMENTS }
DROP DICTIONARY nb_bad;

CREATE DICTIONARY nb_bad (ngram String, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_bad_src')) LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token' priors_mode 'uniform' priors [(0, 0.5), (1, 0.5)])) LIFETIME(0);
SELECT dictGet('nb_bad', 'class_id', 'good'); -- { serverError BAD_ARGUMENTS }
DROP DICTIONARY nb_bad;

CREATE DICTIONARY nb_bad (ngram String, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_bad_src')) LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token' priors_mode 'proportional' priors [(0, 0.5), (1, 0.5)])) LIFETIME(0);
SELECT dictGet('nb_bad', 'class_id', 'good'); -- { serverError BAD_ARGUMENTS }
DROP DICTIONARY nb_bad;

-- ---------- Malformed priors / priors_mode ----------

-- priors_mode 'explicit' requires a priors parameter.
CREATE DICTIONARY nb_bad (ngram String, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_bad_src')) LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token' priors_mode 'explicit')) LIFETIME(0);
SELECT dictGet('nb_bad', 'class_id', 'good'); -- { serverError BAD_ARGUMENTS }
DROP DICTIONARY nb_bad;

-- An empty priors collection is rejected.
CREATE DICTIONARY nb_bad (ngram String, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_bad_src')) LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token' priors_mode 'explicit' priors [])) LIFETIME(0);
SELECT dictGet('nb_bad', 'class_id', 'good'); -- { serverError BAD_ARGUMENTS }
DROP DICTIONARY nb_bad;

-- A string value for priors (for example a hand-written specification) is rejected: the priors must be
-- a collection of (class, probability) pairs.
CREATE DICTIONARY nb_bad (ngram String, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_bad_src')) LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token' priors_mode 'explicit' priors '0=0.5,1=0.5')) LIFETIME(0);
SELECT dictGet('nb_bad', 'class_id', 'good'); -- { serverError BAD_ARGUMENTS }
DROP DICTIONARY nb_bad;

-- A priors collection whose elements are not (key, value) pairs is rejected when the dictionary is created.
CREATE DICTIONARY nb_bad (ngram String, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_bad_src')) LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token' priors_mode 'explicit' priors [0.5, 0.5])) LIFETIME(0); -- { serverError BAD_ARGUMENTS }
DROP DICTIONARY IF EXISTS nb_bad;

-- A non-numeric class id in priors is rejected.
CREATE DICTIONARY nb_bad (ngram String, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_bad_src')) LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token' priors_mode 'explicit' priors [('abc', 0.5), (1, 0.5)])) LIFETIME(0);
SELECT dictGet('nb_bad', 'class_id', 'good'); -- { serverError BAD_ARGUMENTS }
DROP DICTIONARY nb_bad;

-- A negative class id in priors is rejected.
CREATE DICTIONARY nb_bad (ngram String, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_bad_src')) LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token' priors_mode 'explicit' priors [(-1, 0.5), (1, 0.5)])) LIFETIME(0);
SELECT dictGet('nb_bad', 'class_id', 'good'); -- { serverError BAD_ARGUMENTS }
DROP DICTIONARY nb_bad;

-- A non-numeric probability in priors is rejected.
CREATE DICTIONARY nb_bad (ngram String, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_bad_src')) LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token' priors_mode 'explicit' priors [(0, 'abc'), (1, 0.5)])) LIFETIME(0);
SELECT dictGet('nb_bad', 'class_id', 'good'); -- { serverError BAD_ARGUMENTS }
DROP DICTIONARY nb_bad;

-- A duplicate class in priors is rejected.
CREATE DICTIONARY nb_bad (ngram String, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_bad_src')) LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token' priors_mode 'explicit' priors [(0, 0.5), (0, 0.5)])) LIFETIME(0);
SELECT dictGet('nb_bad', 'class_id', 'good'); -- { serverError BAD_ARGUMENTS }
DROP DICTIONARY nb_bad;

-- A priors class id beyond the 32-bit maximum is rejected, not wrapped onto another class.
CREATE DICTIONARY nb_bad (ngram String, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_bad_src')) LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token' priors_mode 'explicit' priors [(4294967296, 0.5), (1, 0.5)])) LIFETIME(0);
SELECT dictGet('nb_bad', 'class_id', 'good'); -- { serverError BAD_ARGUMENTS }
DROP DICTIONARY nb_bad;

-- A class id past even the 64-bit range becomes a floating-point literal and is rejected as a non-integer
-- class id, not wrapped to a small class.
CREATE DICTIONARY nb_bad (ngram String, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_bad_src')) LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token' priors_mode 'explicit' priors [(18446744073709551616, 0.5), (1, 0.5)])) LIFETIME(0);
SELECT dictGet('nb_bad', 'class_id', 'good'); -- { serverError BAD_ARGUMENTS }
DROP DICTIONARY nb_bad;

-- An unknown priors_mode value is rejected.
CREATE DICTIONARY nb_bad (ngram String, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_bad_src')) LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token' priors_mode 'bogus')) LIFETIME(0);
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

-- ---------- All-zero counts (every row records no observation, so the model is empty) ----------

DROP TABLE IF EXISTS nb_zero_src;
CREATE TABLE nb_zero_src (class_id UInt32, ngram String, count UInt64) ENGINE = MergeTree ORDER BY (class_id, ngram);
INSERT INTO nb_zero_src VALUES (0, 'zero', 0), (1, 'one', 0);
CREATE DICTIONARY nb_bad (ngram String, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_zero_src')) LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token')) LIFETIME(0);
SELECT dictGet('nb_bad', 'class_id', 'zero'); -- { serverError RECEIVED_EMPTY_DATA }
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

-- ---------- Total count across classes overflows 64 bits (proportional priors) ----------

-- Each per-class total fits in 64 bits, but proportional priors sum them and that sum overflows.
DROP TABLE IF EXISTS nb_ovf_total_src;
CREATE TABLE nb_ovf_total_src (class_id UInt32, ngram String, count UInt64) ENGINE = MergeTree ORDER BY (class_id, ngram);
INSERT INTO nb_ovf_total_src VALUES (0, 'zero', 18446744073709551615), (1, 'one', 1);
CREATE DICTIONARY nb_bad (ngram String, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_ovf_total_src')) LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token' priors_mode 'proportional')) LIFETIME(0);
SELECT dictGet('nb_bad', 'class_id', 'zero'); -- { serverError BAD_ARGUMENTS }
DROP DICTIONARY nb_bad;
DROP TABLE nb_ovf_total_src;

-- ---------- Configured n does not match the source n-grams ----------

-- nb_bad_src holds unigrams, so loading them as bigrams is rejected.
CREATE DICTIONARY nb_bad (ngram String, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_bad_src')) LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 2 mode 'token')) LIFETIME(0);
SELECT dictGet('nb_bad', 'class_id', 'good'); -- { serverError BAD_ARGUMENTS }
DROP DICTIONARY nb_bad;

-- A single malformed n-gram is rejected even when it appears only near the end of a large source, because
-- every source row is validated. It is ordered last so that a check examining only an early sample of rows
-- would miss it.
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

-- ---------- n-gram size out of range ----------

-- n above the supported maximum (1024) is rejected, because query-time work is quadratic in n.
CREATE DICTIONARY nb_bad (ngram String, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_bad_src')) LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1025 mode 'token')) LIFETIME(0);
SELECT dictGet('nb_bad', 'class_id', 'good'); -- { serverError BAD_ARGUMENTS }
DROP DICTIONARY nb_bad;

-- A value above 2^32 must be read in full, not truncated to a valid small n, so it is still rejected.
CREATE DICTIONARY nb_bad (ngram String, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_bad_src')) LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 4294967297 mode 'token')) LIFETIME(0);
SELECT dictGet('nb_bad', 'class_id', 'good'); -- { serverError BAD_ARGUMENTS }
DROP DICTIONARY nb_bad;

-- ---------- codepoint mode rejects invalid UTF-8 ----------

-- The bytes C2 41 are not valid UTF-8 (C2 must be followed by a continuation byte), so a codepoint model
-- rejects them.
DROP TABLE IF EXISTS nb_utf8_src;
CREATE TABLE nb_utf8_src (class_id UInt32, ngram String, count UInt64) ENGINE = MergeTree ORDER BY (class_id, ngram);
INSERT INTO nb_utf8_src VALUES (0, unhex('C241'), 100), (1, 'AB', 1);
CREATE DICTIONARY nb_bad (ngram String, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_utf8_src')) LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 2 mode 'codepoint')) LIFETIME(0);
SELECT dictGet('nb_bad', 'class_id', 'AB'); -- { serverError BAD_ARGUMENTS }
DROP DICTIONARY nb_bad;

-- byte mode treats every byte literally, so the same source loads and classifies to one of its classes.
SELECT 'byte mode accepts the same bytes';
CREATE DICTIONARY nb_byte_ok (ngram String, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_utf8_src')) LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 2 mode 'byte')) LIFETIME(0);
SELECT naiveBayesClassifier('nb_byte_ok', unhex('C241')) IN (0, 1);
DROP DICTIONARY nb_byte_ok;
DROP TABLE nb_utf8_src;

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

-- ---------- Invalid padding tokens (start_token / end_token) ----------

-- A byte-mode padding token must be a byte value in [0, 255].
CREATE DICTIONARY nb_bad (ngram String, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_bad_src')) LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 2 mode 'byte' start_token '300' end_token '0x01')) LIFETIME(0);
SELECT dictGet('nb_bad', 'class_id', 'good'); -- { serverError BAD_ARGUMENTS }
DROP DICTIONARY nb_bad;

-- A byte-mode padding token must be numeric.
CREATE DICTIONARY nb_bad (ngram String, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_bad_src')) LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 2 mode 'byte' start_token 'zz' end_token '0x01')) LIFETIME(0);
SELECT dictGet('nb_bad', 'class_id', 'good'); -- { serverError BAD_ARGUMENTS }
DROP DICTIONARY nb_bad;

-- A codepoint-mode padding token must not be a UTF-16 surrogate.
CREATE DICTIONARY nb_bad (ngram String, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_bad_src')) LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 2 mode 'codepoint' start_token '0xD800' end_token '0x10FFFF')) LIFETIME(0);
SELECT dictGet('nb_bad', 'class_id', 'good'); -- { serverError BAD_ARGUMENTS }
DROP DICTIONARY nb_bad;

-- A codepoint-mode padding token must be a valid code point (<= 0x10FFFF).
CREATE DICTIONARY nb_bad (ngram String, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_bad_src')) LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 2 mode 'codepoint' start_token '0x110000' end_token '0x1')) LIFETIME(0);
SELECT dictGet('nb_bad', 'class_id', 'good'); -- { serverError BAD_ARGUMENTS }
DROP DICTIONARY nb_bad;

-- A token-mode padding token must not contain whitespace.
CREATE DICTIONARY nb_bad (ngram String, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_bad_src')) LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 2 mode 'token' start_token 'a b' end_token 'c')) LIFETIME(0);
SELECT dictGet('nb_bad', 'class_id', 'good'); -- { serverError BAD_ARGUMENTS }
DROP DICTIONARY nb_bad;

DROP TABLE nb_bad_src;
