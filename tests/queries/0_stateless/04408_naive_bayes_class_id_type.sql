-- dictGet on a NAIVE_BAYES dictionary returns the predicted class in the declared class-attribute type,
-- for every accepted unsigned width (UInt8/16/32/64). The dedicated function always returns UInt32.

DROP TABLE IF EXISTS nb_ct_src;
DROP DICTIONARY IF EXISTS nb_ct8;
DROP DICTIONARY IF EXISTS nb_ct16;
DROP DICTIONARY IF EXISTS nb_ct32;
DROP DICTIONARY IF EXISTS nb_ct64;

CREATE TABLE nb_ct_src (class_id UInt32, ngram String, count UInt64) ENGINE = MergeTree ORDER BY (class_id, ngram);
INSERT INTO nb_ct_src VALUES (0, 'good', 10), (0, 'great', 8), (1, 'bad', 10), (1, 'awful', 6);

CREATE DICTIONARY nb_ct8 (ngram String, class_id UInt8 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_ct_src')) LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token')) LIFETIME(0);
CREATE DICTIONARY nb_ct16 (ngram String, class_id UInt16 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_ct_src')) LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token')) LIFETIME(0);
CREATE DICTIONARY nb_ct32 (ngram String, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_ct_src')) LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token')) LIFETIME(0);
CREATE DICTIONARY nb_ct64 (ngram String, class_id UInt64 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_ct_src')) LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token')) LIFETIME(0);

-- The predicted class comes back in the declared type, with the correct value.
SELECT dictGet('nb_ct8',  'class_id', 'bad')  AS v, toTypeName(v);
SELECT dictGet('nb_ct16', 'class_id', 'bad')  AS v, toTypeName(v);
SELECT dictGet('nb_ct32', 'class_id', 'good') AS v, toTypeName(v);
SELECT dictGet('nb_ct64', 'class_id', 'good') AS v, toTypeName(v);

-- The dedicated function returns UInt32 regardless of the declared class type.
SELECT naiveBayesClassifier('nb_ct8', 'bad') AS v, toTypeName(v);

DROP DICTIONARY nb_ct8;
DROP DICTIONARY nb_ct16;
DROP DICTIONARY nb_ct32;
DROP DICTIONARY nb_ct64;
DROP TABLE nb_ct_src;
