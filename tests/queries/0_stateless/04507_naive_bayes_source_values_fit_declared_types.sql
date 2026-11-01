-- Source class ids and counts are validated against the declared attribute types when the dictionary
-- loads: the training pipeline reads them at full width, so a value that does not fit the declared
-- type fails the load instead of being silently wrapped by the schema cast (300 must not train as 44).
-- Values that do fit train normally, and with store_source the rows read back in the declared types.

DROP DICTIONARY IF EXISTS nb_narrow;
DROP TABLE IF EXISTS nb_narrow_src;

CREATE TABLE nb_narrow_src (ngram String, class_id UInt32, count UInt64) ENGINE = MergeTree ORDER BY ngram;
INSERT INTO nb_narrow_src VALUES ('good', 300, 5), ('bad', 1, 5);

-- A class id that does not fit the declared UInt8 fails the load.
CREATE DICTIONARY nb_narrow (ngram String, class_id UInt8 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_narrow_src'))
LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token')) LIFETIME(0);
SELECT dictGet('nb_narrow', 'class_id', 'good'); -- { serverError BAD_ARGUMENTS }
DROP DICTIONARY nb_narrow;

-- A count that does not fit the declared UInt8 fails the load.
TRUNCATE TABLE nb_narrow_src;
INSERT INTO nb_narrow_src VALUES ('good', 0, 1000), ('bad', 1, 5);
CREATE DICTIONARY nb_narrow (ngram String, class_id UInt8 DEFAULT 0, count UInt8 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_narrow_src'))
LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token')) LIFETIME(0);
SELECT dictGet('nb_narrow', 'class_id', 'good'); -- { serverError BAD_ARGUMENTS }
DROP DICTIONARY nb_narrow;

-- A class id above the 32-bit model maximum is rejected even when the declared type holds it.
TRUNCATE TABLE nb_narrow_src;
INSERT INTO nb_narrow_src VALUES ('good', 0, 5), ('bad', 1, 5);
DROP TABLE IF EXISTS nb_wide_src;
CREATE TABLE nb_wide_src (ngram String, class_id UInt64, count UInt64) ENGINE = MergeTree ORDER BY ngram;
INSERT INTO nb_wide_src VALUES ('good', 4294967296, 5);
CREATE DICTIONARY nb_narrow (ngram String, class_id UInt64 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_wide_src'))
LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token')) LIFETIME(0);
SELECT dictGet('nb_narrow', 'class_id', 'good'); -- { serverError BAD_ARGUMENTS }
DROP DICTIONARY nb_narrow;

-- Narrow declared types work when every source value fits, and store_source reads the rows back in
-- the declared types.
CREATE DICTIONARY nb_narrow (ngram String, class_id UInt8 DEFAULT 0, count UInt16 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_narrow_src'))
LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token' store_source 1)) LIFETIME(0);
SELECT dictGet('nb_narrow', 'class_id', 'bad') AS v, toTypeName(v);
SELECT ngram, class_id, count, toTypeName(class_id), toTypeName(count) FROM nb_narrow ORDER BY ngram;

DROP DICTIONARY nb_narrow;
DROP TABLE nb_narrow_src;
DROP TABLE nb_wide_src;
