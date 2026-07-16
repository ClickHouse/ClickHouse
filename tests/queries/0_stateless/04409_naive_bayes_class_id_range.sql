-- The model represents class ids as 32-bit values, so a class id that does not fit is rejected at load,
-- while the largest fitting class id (UInt32 max) is accepted and returned unchanged.

DROP TABLE IF EXISTS nb_cid_src;
DROP DICTIONARY IF EXISTS nb_cid_big;
DROP DICTIONARY IF EXISTS nb_cid_max;

CREATE TABLE nb_cid_src (class_id UInt64, ngram String, count UInt64) ENGINE = MergeTree ORDER BY (class_id, ngram);

-- A class id of 2^32 does not fit in 32 bits.
INSERT INTO nb_cid_src VALUES (4294967296, 'wide', 10), (0, 'zero', 10);
CREATE DICTIONARY nb_cid_big (ngram String, class_id UInt64 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_cid_src')) LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token')) LIFETIME(0);
SELECT naiveBayesClassifier('nb_cid_big', 'wide'); -- { serverError BAD_ARGUMENTS }
DROP DICTIONARY nb_cid_big;
DROP TABLE nb_cid_src;

-- The largest class id that fits (UInt32 max) loads and is returned unchanged.
DROP TABLE IF EXISTS nb_cid_src;
CREATE TABLE nb_cid_src (class_id UInt64, ngram String, count UInt64) ENGINE = MergeTree ORDER BY (class_id, ngram);
INSERT INTO nb_cid_src VALUES (4294967295, 'wide', 10), (0, 'zero', 10);
CREATE DICTIONARY nb_cid_max (ngram String, class_id UInt64 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_cid_src')) LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token')) LIFETIME(0);
SELECT naiveBayesClassifier('nb_cid_max', 'wide');
DROP DICTIONARY nb_cid_max;
DROP TABLE nb_cid_src;
