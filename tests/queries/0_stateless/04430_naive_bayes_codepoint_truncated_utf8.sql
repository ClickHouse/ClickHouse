-- A truncated or malformed leading byte in codepoint mode is tokenized leniently, and the n-gram view stays
-- within the input buffer: the per-codepoint byte offsets are clamped to the buffer size, so a multi-byte
-- sequence whose declared length runs past the end of the input is never read beyond it.

-- A lone 4-byte lead byte (0xF0) with no continuation bytes: one n-gram, exactly the single input byte.
SELECT arrayMap(x -> hex(x), naiveBayesNgrams(unhex('F0'), 1, 'codepoint'));

-- A truncated lead byte followed by ASCII: the malformed sequence merges with the following bytes (lenient
-- query-time tokenization), and the whole result is exactly the five input bytes, with nothing read past them.
SELECT arrayMap(x -> hex(x), naiveBayesNgrams(unhex('F0') || 'ABCD', 1, 'codepoint'));

-- The same input through the classifier: the malformed n-gram is out-of-vocabulary, so it is dropped and the
-- prediction is the prior argmax (class 0). The point is that classifying a truncated byte neither reads past
-- the input nor raises an exception.
DROP DICTIONARY IF EXISTS nb_trunc;
DROP TABLE IF EXISTS nb_trunc_src;

CREATE TABLE nb_trunc_src (ngram String, class_id UInt32, count UInt64) ENGINE = Memory;
INSERT INTO nb_trunc_src VALUES ('a', 0, 10), ('b', 1, 10);

CREATE DICTIONARY nb_trunc (ngram String, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram
SOURCE(CLICKHOUSE(TABLE 'nb_trunc_src'))
LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'codepoint'))
LIFETIME(0);

SELECT naiveBayesClassifier('nb_trunc', unhex('F0'));

DROP DICTIONARY nb_trunc;
DROP TABLE nb_trunc_src;
