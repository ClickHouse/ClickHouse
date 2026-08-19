-- Padding (start_token / end_token) is opt-in. Without it the input is tokenized as-is; with it the input is
-- padded by (n-1) boundary tokens at each end, so a model trained with boundary n-grams uses them. Each mode
-- below has boundary n-grams in its source and classifies the same input differently with vs without padding:
-- without padding the single interior bigram 'xy' wins class 1; with padding the two boundary bigrams win
-- class 0. Byte/codepoint tokens are given as numbers (decimal or 0x hex); token mode takes a literal token.

-- ---------- byte mode ----------
DROP TABLE IF EXISTS nb_pad_byte;
CREATE TABLE nb_pad_byte (class_id UInt32, ngram String, count UInt64) ENGINE = MergeTree ORDER BY (class_id, ngram);
INSERT INTO nb_pad_byte VALUES (0, '\x01x', 9), (1, 'xy', 9), (0, 'y\xFF', 9);

DROP DICTIONARY IF EXISTS nb_byte_nopad;
CREATE DICTIONARY nb_byte_nopad (ngram String, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_pad_byte')) LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 2 mode 'byte')) LIFETIME(0);

DROP DICTIONARY IF EXISTS nb_byte_pad;
CREATE DICTIONARY nb_byte_pad (ngram String, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_pad_byte')) LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 2 mode 'byte' start_token '0x01' end_token '0xFF')) LIFETIME(0);

SELECT 'byte', naiveBayesClassifier('nb_byte_nopad', 'xy'), naiveBayesClassifier('nb_byte_pad', 'xy');

-- A byte token accepts decimal or 0x hex for the same byte, producing an identical model.
DROP DICTIONARY IF EXISTS nb_byte_pad_dec;
CREATE DICTIONARY nb_byte_pad_dec (ngram String, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_pad_byte')) LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 2 mode 'byte' start_token '1' end_token '255')) LIFETIME(0);
SELECT 'byte hex==dec', naiveBayesClassifier('nb_byte_pad', 'xy') = naiveBayesClassifier('nb_byte_pad_dec', 'xy');

-- A shared boundary marker (start_token == end_token) is allowed; leading and trailing boundary n-grams still
-- differ by the marker's position. It must load and classify without error.
DROP DICTIONARY IF EXISTS nb_byte_same;
CREATE DICTIONARY nb_byte_same (ngram String, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_pad_byte')) LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 2 mode 'byte' start_token '0x01' end_token '0x01')) LIFETIME(0);
SELECT 'byte shared-boundary', naiveBayesClassifier('nb_byte_same', 'xy');
DROP DICTIONARY nb_byte_same;

-- Padding is per-side: start_token and end_token are independent, so one-sided padding is allowed.
DROP DICTIONARY IF EXISTS nb_byte_start_only;
CREATE DICTIONARY nb_byte_start_only (ngram String, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_pad_byte')) LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 2 mode 'byte' start_token '0x01')) LIFETIME(0);
DROP DICTIONARY IF EXISTS nb_byte_end_only;
CREATE DICTIONARY nb_byte_end_only (ngram String, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_pad_byte')) LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 2 mode 'byte' end_token '0xFF')) LIFETIME(0);
SELECT 'byte start-only', naiveBayesClassifier('nb_byte_start_only', 'xy');
SELECT 'byte end-only', naiveBayesClassifier('nb_byte_end_only', 'xy');

-- An empty token value is treated as "not given": start_token '' with end_token '0xFF' is identical to
-- specifying only end_token '0xFF'.
DROP DICTIONARY IF EXISTS nb_byte_empty_start;
CREATE DICTIONARY nb_byte_empty_start (ngram String, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_pad_byte')) LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 2 mode 'byte' start_token '' end_token '0xFF')) LIFETIME(0);
SELECT 'byte empty==omitted', naiveBayesClassifier('nb_byte_empty_start', 'xy') = naiveBayesClassifier('nb_byte_end_only', 'xy');
DROP DICTIONARY nb_byte_start_only;
DROP DICTIONARY nb_byte_end_only;
DROP DICTIONARY nb_byte_empty_start;

-- ---------- codepoint mode ----------
DROP TABLE IF EXISTS nb_pad_cp;
CREATE TABLE nb_pad_cp (class_id UInt32, ngram String, count UInt64) ENGINE = MergeTree ORDER BY (class_id, ngram);
-- U+10FFFE = F4 8F BF BE, U+10FFFF = F4 8F BF BF
INSERT INTO nb_pad_cp VALUES (0, '\xF4\x8F\xBF\xBEx', 9), (1, 'xy', 9), (0, 'y\xF4\x8F\xBF\xBF', 9);

DROP DICTIONARY IF EXISTS nb_cp_nopad;
CREATE DICTIONARY nb_cp_nopad (ngram String, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_pad_cp')) LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 2 mode 'codepoint')) LIFETIME(0);

DROP DICTIONARY IF EXISTS nb_cp_pad;
CREATE DICTIONARY nb_cp_pad (ngram String, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_pad_cp')) LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 2 mode 'codepoint' start_token '0x10FFFE' end_token '0x10FFFF')) LIFETIME(0);

SELECT 'codepoint', naiveBayesClassifier('nb_cp_nopad', 'xy'), naiveBayesClassifier('nb_cp_pad', 'xy');

-- ---------- token mode ----------
DROP TABLE IF EXISTS nb_pad_tok;
CREATE TABLE nb_pad_tok (class_id UInt32, ngram String, count UInt64) ENGINE = MergeTree ORDER BY (class_id, ngram);
INSERT INTO nb_pad_tok VALUES (0, '<s> good', 9), (1, 'good bad', 9), (0, 'bad </s>', 9);

DROP DICTIONARY IF EXISTS nb_tok_nopad;
CREATE DICTIONARY nb_tok_nopad (ngram String, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_pad_tok')) LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 2 mode 'token')) LIFETIME(0);

DROP DICTIONARY IF EXISTS nb_tok_pad;
CREATE DICTIONARY nb_tok_pad (ngram String, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_pad_tok')) LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 2 mode 'token' start_token '<s>' end_token '</s>')) LIFETIME(0);

SELECT 'token', naiveBayesClassifier('nb_tok_nopad', 'good bad'), naiveBayesClassifier('nb_tok_pad', 'good bad');

DROP DICTIONARY nb_byte_nopad; DROP DICTIONARY nb_byte_pad; DROP DICTIONARY nb_byte_pad_dec; DROP TABLE nb_pad_byte;
DROP DICTIONARY nb_cp_nopad; DROP DICTIONARY nb_cp_pad; DROP TABLE nb_pad_cp;
DROP DICTIONARY nb_tok_nopad; DROP DICTIONARY nb_tok_pad; DROP TABLE nb_pad_tok;
