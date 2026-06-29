-- A NAIVE_BAYES dictionary classifies every key, so dictGetOrDefault never uses the default. This must also
-- hold on the short-circuit path (a lazy / non-constant default), where the dictionary has to fill the default
-- filter; otherwise the call fails with a column-size mismatch.
DROP DICTIONARY IF EXISTS nb_god_dict;
DROP TABLE IF EXISTS nb_god_src;

CREATE TABLE nb_god_src (ngram String, class_id UInt32, count UInt64) ENGINE = MergeTree ORDER BY (class_id, ngram);
INSERT INTO nb_god_src VALUES ('good', 0, 5), ('great', 0, 4), ('bad', 1, 5), ('awful', 1, 4);

CREATE DICTIONARY nb_god_dict (ngram String, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_god_src'))
LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token')) LIFETIME(0);

-- Constant default (eager path): the default is ignored and the classifier result is returned.
SELECT dictGetOrDefault('nb_god_dict', 'class_id', 'good great', toUInt32(99));

-- Lazy / non-constant default with short-circuit forced: still returns the classifier result, not the default.
SELECT t, dictGetOrDefault('nb_god_dict', 'class_id', t, materialize(toUInt32(99))) AS r
FROM (SELECT arrayJoin(['good great', 'bad awful']) AS t)
ORDER BY t
SETTINGS short_circuit_function_evaluation = 'force_enable';

DROP DICTIONARY nb_god_dict;
DROP TABLE nb_god_src;
