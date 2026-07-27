-- naiveBayesClassifier returns the same class value as dictGet on the class attribute, but differs in return type and
-- in which key types it accepts.

DROP DICTIONARY IF EXISTS nb_cmp;
DROP TABLE IF EXISTS nb_cmp_src;

CREATE TABLE nb_cmp_src (ngram String, class_id UInt32, count UInt64) ENGINE = Memory;
INSERT INTO nb_cmp_src VALUES ('good', 0, 10), ('bad', 1, 10);

-- The class attribute is declared UInt8, narrower than the model's internal UInt32.
CREATE DICTIONARY nb_cmp (ngram String, class_id UInt8 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram
SOURCE(CLICKHOUSE(TABLE 'nb_cmp_src'))
LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token'))
LIFETIME(0);

-- Same class value, but naiveBayesClassifier always returns UInt32 while dictGet returns the declared attribute type.
SELECT naiveBayesClassifier('nb_cmp', 'bad') = dictGet('nb_cmp', 'class_id', 'bad');
SELECT toTypeName(naiveBayesClassifier('nb_cmp', 'bad'));
SELECT toTypeName(dictGet('nb_cmp', 'class_id', 'bad'));

-- dictGet converts a non-String key to String via dictionary key conversion; naiveBayesClassifier requires String input.
SELECT toTypeName(dictGet('nb_cmp', 'class_id', toUInt64(123)));
SELECT naiveBayesClassifier('nb_cmp', toUInt64(123)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

DROP DICTIONARY nb_cmp;
DROP TABLE nb_cmp_src;

-- In codepoint mode, query input that is not valid UTF-8 is tokenized best-effort, so both functions classify it
-- instead of failing the query.
DROP DICTIONARY IF EXISTS nb_cp;
DROP TABLE IF EXISTS nb_cp_src;

CREATE TABLE nb_cp_src (ngram String, class_id UInt32, count UInt64) ENGINE = Memory;
INSERT INTO nb_cp_src VALUES ('a', 0, 10), ('b', 1, 10);

CREATE DICTIONARY nb_cp (ngram String, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram
SOURCE(CLICKHOUSE(TABLE 'nb_cp_src'))
LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'codepoint'))
LIFETIME(0);

SELECT naiveBayesClassifier('nb_cp', unhex('C241')) IN (0, 1);
SELECT dictGet('nb_cp', 'class_id', unhex('C241')) IN (0, 1);

DROP DICTIONARY nb_cp;
DROP TABLE nb_cp_src;
