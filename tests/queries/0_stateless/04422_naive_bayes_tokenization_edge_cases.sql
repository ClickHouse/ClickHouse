-- Tokenization edge cases that the NAIVE_BAYES documentation describes.

-- token mode splits on ASCII whitespace only.
SELECT naiveBayesNgrams('a b', 1, 'token');                                                    -- ASCII space splits
SELECT naiveBayesNgrams(concat('a', '\t', 'b'), 1, 'token');                                   -- tab splits
SELECT arrayMap(x -> hex(x), naiveBayesNgrams(concat('a', unhex('C2A0'), 'b'), 1, 'token'));   -- U+00A0 no-break space stays inside one token
SELECT arrayMap(x -> hex(x), naiveBayesNgrams(concat('a', unhex('E28083'), 'b'), 1, 'token')); -- U+2003 em space stays inside one token
SELECT length(naiveBayesNgrams(concat('a', unhex('C2A0'), 'b'), 1, 'token'));                  -- one token, not two

-- codepoint mode at query time decodes invalid UTF-8 best-effort rather than rejecting it.
SELECT arrayMap(x -> hex(x), naiveBayesNgrams(unhex('C241'), 1, 'codepoint'));                 -- C2 41 forms one best-effort code point
SELECT length(naiveBayesNgrams(unhex('C241'), 1, 'codepoint'));                                -- one n-gram, no error

-- the source loader rejects invalid UTF-8 in codepoint mode, unlike query-time tokenization.
DROP DICTIONARY IF EXISTS nb_cp_invalid;
DROP TABLE IF EXISTS nb_cp_invalid_src;

CREATE TABLE nb_cp_invalid_src (ngram String, class_id UInt32, count UInt64) ENGINE = Memory;
INSERT INTO nb_cp_invalid_src VALUES (unhex('C241'), 0, 10), ('B', 1, 10);

CREATE DICTIONARY nb_cp_invalid (ngram String, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram
SOURCE(CLICKHOUSE(TABLE 'nb_cp_invalid_src'))
LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'codepoint'))
LIFETIME(0);

SELECT naiveBayesClassifier('nb_cp_invalid', 'B'); -- { serverError BAD_ARGUMENTS }

DROP DICTIONARY nb_cp_invalid;
DROP TABLE nb_cp_invalid_src;
