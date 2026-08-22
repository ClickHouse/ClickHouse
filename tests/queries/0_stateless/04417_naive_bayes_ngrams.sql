-- Tokenization matches the NAIVE_BAYES dictionary: token / byte / codepoint, with optional boundary padding.

-- ---- token mode ----
SELECT naiveBayesNgrams('the cat sat', 1, 'token');
SELECT naiveBayesNgrams('the cat sat', 2, 'token');
SELECT naiveBayesNgrams('  the   cat  ', 1, 'token');                       -- leading/trailing/repeated whitespace collapses
SELECT naiveBayesNgrams(concat('a', '\t', 'b', '\n', 'c'), 1, 'token');     -- tabs and newlines are token separators too
SELECT naiveBayesNgrams('a, b.', 1, 'token');                               -- punctuation stays part of the token
SELECT naiveBayesNgrams(materialize('the cat sat'), 2, 'token');            -- non-const input

-- ---- byte mode ----
SELECT naiveBayesNgrams('abc', 1, 'byte');
SELECT naiveBayesNgrams('abc', 2, 'byte');
SELECT naiveBayesNgrams('abc', 3, 'byte');                                  -- n equal to the input length: one n-gram
SELECT arrayMap(x -> hex(x), naiveBayesNgrams('é', 1, 'byte'));             -- bytes, not code points (é is C3 A9)

-- ---- codepoint mode ----
SELECT naiveBayesNgrams('café', 1, 'codepoint');
SELECT naiveBayesNgrams('héllo', 2, 'codepoint');
SELECT naiveBayesNgrams('é', 1, 'codepoint');                              -- one multi-byte code point
SELECT naiveBayesNgrams('日本語', 2, 'codepoint');                          -- 3-byte code points
SELECT naiveBayesNgrams('🎉🎈', 1, 'codepoint');                            -- 4-byte code points
SELECT naiveBayesNgrams('héllo', 2, 'codepoint') = ngrams('héllo', 2);     -- equivalent to the built-in ngrams
SELECT naiveBayesNgrams('ClickHouse', 3, 'codepoint') = ngrams('ClickHouse', 3);

-- ---- boundary padding ----
SELECT naiveBayesNgrams('cat', 2, 'token', '<s>', '</s>');                  -- token mode: literal tokens
SELECT arrayMap(x -> hex(x), naiveBayesNgrams('xy', 2, 'byte', '0x01', '0xFF'));   -- byte mode: numeric tokens (both sides)
SELECT arrayMap(x -> hex(x), naiveBayesNgrams('xy', 2, 'byte', '0x01', ''));       -- start side only
SELECT arrayMap(x -> hex(x), naiveBayesNgrams('xy', 2, 'byte', '', '0xFF'));       -- end side only
SELECT naiveBayesNgrams('ab', 2, 'codepoint', '0x5E', '0x24');             -- codepoint mode: numeric tokens (^ and $)
SELECT naiveBayesNgrams('xy', 2, 'byte', '1', '255') = naiveBayesNgrams('xy', 2, 'byte', '0x01', '0xFF');  -- decimal == 0x hex
SELECT naiveBayesNgrams('xy', 2, 'byte', '', '') = naiveBayesNgrams('xy', 2, 'byte');                      -- empty token == no padding
SELECT naiveBayesNgrams('ab', 1, 'byte', '0x01', '0xFF') = naiveBayesNgrams('ab', 1, 'byte');             -- padding has no effect for n = 1

-- ---- short / empty input and the n upper bound ----
SELECT naiveBayesNgrams('a', 2, 'byte');                                    -- input shorter than n: no n-grams
SELECT naiveBayesNgrams('', 1, 'token');
SELECT length(naiveBayesNgrams('abc', 1024, 'byte'));                       -- n = 1024 is accepted (no n-grams here)

-- ---- per-row vectorization: offsets across rows of varying length ----
SELECT s, naiveBayesNgrams(s, 2, 'byte') FROM (SELECT arrayJoin(['', 'a', 'ab', 'abc']) AS s) ORDER BY s;
SELECT s, naiveBayesNgrams(s, 2, 'token') FROM (SELECT arrayJoin(['', 'one', 'one two', 'one two three']) AS s) ORDER BY s;

-- ---- return type ----
SELECT toTypeName(naiveBayesNgrams('a', 1, 'byte'));

-- ---- round-trip: build training data from raw (class_id, text) with naiveBayesNgrams + GROUP BY,
-- create a NAIVE_BAYES dictionary from it, and classify - the function matches the dictionary tokenizer ----
DROP DICTIONARY IF EXISTS nb_rt_dict;
DROP TABLE IF EXISTS nb_rt_docs;
DROP TABLE IF EXISTS nb_rt_train;

CREATE TABLE nb_rt_docs (class_id UInt32, text String) ENGINE = MergeTree ORDER BY class_id;
INSERT INTO nb_rt_docs VALUES (0, 'good great wonderful'), (0, 'great good nice'), (1, 'bad terrible awful'), (1, 'terrible bad horrible');

CREATE TABLE nb_rt_train (ngram String, class_id UInt32, count UInt64) ENGINE = MergeTree ORDER BY (class_id, ngram);
INSERT INTO nb_rt_train
SELECT ngram, class_id, count()
FROM nb_rt_docs ARRAY JOIN naiveBayesNgrams(text, 1, 'token') AS ngram
GROUP BY ngram, class_id;

CREATE DICTIONARY nb_rt_dict (ngram String, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_rt_train'))
LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token' priors_mode 'uniform')) LIFETIME(0);

SELECT naiveBayesClassifier('nb_rt_dict', 'good great');
SELECT naiveBayesClassifier('nb_rt_dict', 'bad awful');

DROP DICTIONARY nb_rt_dict;
DROP TABLE nb_rt_train;
DROP TABLE nb_rt_docs;

-- ---- errors ----
SELECT naiveBayesNgrams('x', 0, 'byte'); -- { serverError BAD_ARGUMENTS }
SELECT naiveBayesNgrams('x', 1025, 'byte'); -- { serverError BAD_ARGUMENTS }
SELECT naiveBayesNgrams('x', 2, 'bogus'); -- { serverError BAD_ARGUMENTS }
SELECT naiveBayesNgrams('x', 2, 'byte', '256'); -- { serverError BAD_ARGUMENTS }
SELECT naiveBayesNgrams('x', 2, 'codepoint', '0xD800'); -- { serverError BAD_ARGUMENTS }
SELECT naiveBayesNgrams('a b', 2, 'token', 'x y'); -- { serverError BAD_ARGUMENTS }
SELECT naiveBayesNgrams('x', 2); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT naiveBayesNgrams('x', 2, 'byte', '0x01', '0xFF', 'extra'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT naiveBayesNgrams('ab', materialize(toUInt32(2)), 'byte'); -- { serverError ILLEGAL_COLUMN }
SELECT naiveBayesNgrams('ab', 2, materialize('byte')); -- { serverError ILLEGAL_COLUMN }
SELECT naiveBayesNgrams(123, 2, 'byte'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT naiveBayesNgrams('ab', 'x', 'byte'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT naiveBayesNgrams(toFixedString('abc', 3), 2, 'byte'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
