-- Exercises the dictionary-management features of the NAIVE_BAYES format: the metadata it reports
-- in system.dictionaries, that SYSTEM RELOAD DICTIONARY rebuilds the model from updated source data,
-- and that the dictionary disappears and can no longer classify once it is dropped.

DROP DICTIONARY IF EXISTS nb_life;
DROP TABLE IF EXISTS nb_life_src;

CREATE TABLE nb_life_src (class_id UInt32, ngram String, count UInt64) ENGINE = MergeTree ORDER BY (class_id, ngram);
INSERT INTO nb_life_src VALUES (0, 'good', 10), (0, 'great', 8), (1, 'bad', 10), (1, 'awful', 6);

CREATE DICTIONARY nb_life
(
    ngram String,
    class_id UInt32 DEFAULT 0,
    count UInt64 DEFAULT 0
)
PRIMARY KEY ngram
SOURCE(CLICKHOUSE(TABLE 'nb_life_src' DB currentDatabase()))
LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token'))
LIFETIME(0);

-- Classifying 'good' lazily loads the dictionary. It is trained only for class 0, so it returns 0.
SELECT 'classify before update';
SELECT naiveBayesClassifier('nb_life', 'good');

-- Once loaded, the format reports itself as a loaded NaiveBayes dictionary with the declared key and attributes.
SELECT 'metadata';
SELECT status, type, key.names, attribute.names, attribute.types
FROM system.dictionaries WHERE database = currentDatabase() AND name = 'nb_life';

-- EXISTS DICTIONARY sees the dictionary.
SELECT 'exists';
EXISTS DICTIONARY nb_life;

-- Add overwhelming class-1 evidence for 'good' to the source. With LIFETIME(0) the dictionary never
-- refreshes on its own, so the classification stays 0 until the dictionary is explicitly reloaded.
INSERT INTO nb_life_src VALUES (1, 'good', 1000);
SELECT 'classify after update, before reload';
SELECT naiveBayesClassifier('nb_life', 'good');

-- Reloading rebuilds the model from the updated source, and now 'good' classifies as 1.
SYSTEM RELOAD DICTIONARY nb_life;
SELECT 'classify after reload';
SELECT naiveBayesClassifier('nb_life', 'good');

-- After dropping, the dictionary no longer exists and can no longer be used to classify.
DROP DICTIONARY nb_life;
SELECT 'exists after drop';
EXISTS DICTIONARY nb_life;
SELECT naiveBayesClassifier('nb_life', 'good'); -- { serverError BAD_ARGUMENTS }

DROP TABLE nb_life_src;


-- The dedicated functions update query_count just like dictGet, so dictionary usage statistics reflect them.
DROP DICTIONARY IF EXISTS nb_qc;
DROP TABLE IF EXISTS nb_qc_src;
CREATE TABLE nb_qc_src (class_id UInt32, ngram String, count UInt64) ENGINE = MergeTree ORDER BY (class_id, ngram);
INSERT INTO nb_qc_src VALUES (0, 'good', 10), (1, 'bad', 10);
CREATE DICTIONARY nb_qc
(
    ngram String,
    class_id UInt32 DEFAULT 0,
    count UInt64 DEFAULT 0
)
PRIMARY KEY ngram
SOURCE(CLICKHOUSE(TABLE 'nb_qc_src' DB currentDatabase()))
LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token'))
LIFETIME(0);

SELECT 'query_count after five function lookups';
SELECT naiveBayesClassifier('nb_qc', concat('w', toString(number))) FROM numbers(5) FORMAT Null;
SELECT query_count FROM system.dictionaries WHERE database = currentDatabase() AND name = 'nb_qc';

SELECT 'query_count after three more dictGet lookups';
SELECT dictGet('nb_qc', 'class_id', concat('w', toString(number))) FROM numbers(3) FORMAT Null;
SELECT query_count FROM system.dictionaries WHERE database = currentDatabase() AND name = 'nb_qc';

DROP DICTIONARY nb_qc;
DROP TABLE nb_qc_src;
