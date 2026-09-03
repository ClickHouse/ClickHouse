-- The naiveBayesClassifier* functions resolve the referenced dictionary during return-type inference,
-- like dictGet: a stored expression (e.g. a column DEFAULT) referencing a missing dictionary or a
-- dictionary of another layout is rejected when it is analyzed, not when it is first evaluated. The
-- check reads the layout from the dictionary configuration, so it does not load the dictionary.

DROP TABLE IF EXISTS nb_defaults;
DROP DICTIONARY IF EXISTS nb_analysis_dict;
DROP DICTIONARY IF EXISTS nb_analysis_flat;
DROP TABLE IF EXISTS nb_analysis_src;
DROP TABLE IF EXISTS nb_analysis_flat_src;

-- A missing dictionary fails the query analysis for all three functions.
SELECT naiveBayesClassifier('nb_analysis_missing', 'x'); -- { serverError BAD_ARGUMENTS }
SELECT naiveBayesClassifierWithProb('nb_analysis_missing', 'x'); -- { serverError BAD_ARGUMENTS }
SELECT naiveBayesClassifierWithAllProbs('nb_analysis_missing', 'x'); -- { serverError BAD_ARGUMENTS }

-- A missing dictionary in a column DEFAULT fails CREATE TABLE, for all three functions.
CREATE TABLE nb_defaults (text String, cls UInt32 DEFAULT naiveBayesClassifier('nb_analysis_missing', text))
ENGINE = MergeTree ORDER BY text; -- { serverError BAD_ARGUMENTS }
CREATE TABLE nb_defaults (text String, wp Tuple(class_id UInt32, probability Float64) DEFAULT naiveBayesClassifierWithProb('nb_analysis_missing', text))
ENGINE = MergeTree ORDER BY text; -- { serverError BAD_ARGUMENTS }
CREATE TABLE nb_defaults (text String, ap Array(Tuple(class_id UInt32, probability Float64)) DEFAULT naiveBayesClassifierWithAllProbs('nb_analysis_missing', text))
ENGINE = MergeTree ORDER BY text; -- { serverError BAD_ARGUMENTS }

-- A dictionary of another layout is rejected the same way, without loading it.
CREATE TABLE nb_analysis_flat_src (id UInt64, val UInt8) ENGINE = MergeTree ORDER BY id;
CREATE DICTIONARY nb_analysis_flat (id UInt64, val UInt8 DEFAULT 0)
PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 'nb_analysis_flat_src')) LAYOUT(FLAT()) LIFETIME(0);

SELECT naiveBayesClassifier('nb_analysis_flat', 'x'); -- { serverError BAD_ARGUMENTS }
CREATE TABLE nb_defaults (text String, cls UInt32 DEFAULT naiveBayesClassifier('nb_analysis_flat', text))
ENGINE = MergeTree ORDER BY text; -- { serverError BAD_ARGUMENTS }

-- A NAIVE_BAYES dictionary in a column DEFAULT works end to end.
CREATE TABLE nb_analysis_src (ngram String, class_id UInt32, count UInt64) ENGINE = MergeTree ORDER BY ngram;
INSERT INTO nb_analysis_src VALUES ('good', 0, 5), ('bad', 1, 5);
CREATE DICTIONARY nb_analysis_dict (ngram String, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_analysis_src'))
LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token')) LIFETIME(0);

CREATE TABLE nb_defaults (text String, cls UInt32 DEFAULT naiveBayesClassifier('nb_analysis_dict', text))
ENGINE = MergeTree ORDER BY text;
INSERT INTO nb_defaults (text) VALUES ('bad day'), ('good day');
SELECT * FROM nb_defaults ORDER BY text;

DROP TABLE nb_defaults;
DROP DICTIONARY nb_analysis_dict;
DROP DICTIONARY nb_analysis_flat;
DROP TABLE nb_analysis_src;
DROP TABLE nb_analysis_flat_src;
