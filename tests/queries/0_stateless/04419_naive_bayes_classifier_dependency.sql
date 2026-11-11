-- naiveBayesClassifier reads a dictionary by name (its first argument), exactly like dictGet, so in a stored
-- expression (here a column DEFAULT) the dictionary name must be qualified with the current database and
-- recorded as a dependency. This guards attach/restore/drop ordering and database resolution.
DROP TABLE IF EXISTS nb_dep_tbl;
DROP DICTIONARY IF EXISTS nb_dep_dict;
DROP TABLE IF EXISTS nb_dep_src;

CREATE TABLE nb_dep_src (ngram String, class_id UInt32, count UInt64) ENGINE = MergeTree ORDER BY (class_id, ngram);
INSERT INTO nb_dep_src VALUES ('good', 0, 5), ('bad', 1, 5);

CREATE DICTIONARY nb_dep_dict (ngram String, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_dep_src'))
LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token')) LIFETIME(0);

CREATE TABLE nb_dep_tbl (text String, cls UInt32 DEFAULT naiveBayesClassifier('nb_dep_dict', text)) ENGINE = MergeTree ORDER BY tuple();

-- The stored expression qualifies the dictionary name with the current database.
SELECT position(create_table_query, concat(currentDatabase(), '.nb_dep_dict')) > 0
FROM system.tables WHERE database = currentDatabase() AND name = 'nb_dep_tbl';

-- The dictionary is recorded as a loading dependency of the table.
SELECT has(loading_dependencies_table, 'nb_dep_dict')
FROM system.tables WHERE database = currentDatabase() AND name = 'nb_dep_tbl';

-- The DEFAULT computes through the dictionary.
INSERT INTO nb_dep_tbl (text) VALUES ('good'), ('bad');
SELECT text, cls FROM nb_dep_tbl ORDER BY text;

-- The recorded dependency blocks dropping the dictionary while the table still depends on it.
DROP DICTIONARY nb_dep_dict; -- { serverError HAVE_DEPENDENT_OBJECTS }

DROP TABLE nb_dep_tbl;
DROP DICTIONARY nb_dep_dict;
DROP TABLE nb_dep_src;
