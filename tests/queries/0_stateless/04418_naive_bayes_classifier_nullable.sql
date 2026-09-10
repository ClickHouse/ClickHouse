-- All three naiveBayesClassifier* functions accept a Nullable(String) input. naiveBayesClassifier and
-- naiveBayesClassifierWithProb propagate NULL (Nullable(UInt32) / Nullable(Tuple); a NULL row stays NULL),
-- while naiveBayesClassifierWithAllProbs cannot be Nullable(Array) and so yields an empty array for a NULL row.
-- The dictionary load and access check always run (executeImpl is reached for every call).
DROP DICTIONARY IF EXISTS nb_nullable_dict;
DROP TABLE IF EXISTS nb_nullable_src;

CREATE TABLE nb_nullable_src (ngram String, class_id UInt32, count UInt64) ENGINE = MergeTree ORDER BY (class_id, ngram);
INSERT INTO nb_nullable_src VALUES ('good', 0, 5), ('great', 0, 4), ('bad', 1, 5), ('awful', 1, 4);

CREATE DICTIONARY nb_nullable_dict (ngram String, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_nullable_src'))
LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token')) LIFETIME(0);

-- A Nullable input makes the scalar and the tuple Nullable (the array stays non-Nullable - it cannot be
-- Nullable - which the empty-array result below demonstrates).
SELECT toTypeName(naiveBayesClassifier('nb_nullable_dict', CAST('good' AS Nullable(String))));
SELECT toTypeName(naiveBayesClassifierWithProb('nb_nullable_dict', CAST('good' AS Nullable(String))));
-- A plain String input keeps the original (non-Nullable) return type.
SELECT toTypeName(naiveBayesClassifier('nb_nullable_dict', 'good'));

-- A NULL input propagates: NULL class, NULL tuple, and an empty array.
SELECT naiveBayesClassifier('nb_nullable_dict', materialize(CAST(NULL AS Nullable(String))));
SELECT isNull(naiveBayesClassifierWithProb('nb_nullable_dict', materialize(CAST(NULL AS Nullable(String)))));
SELECT naiveBayesClassifierWithAllProbs('nb_nullable_dict', materialize(CAST(NULL AS Nullable(String))));

-- Mixed Nullable column across all three variants (NULL rows propagate; probabilities rounded).
SELECT
    t,
    naiveBayesClassifier('nb_nullable_dict', t) AS class,
    isNull(naiveBayesClassifierWithProb('nb_nullable_dict', t)) AS prob_is_null,
    arrayMap(p -> (p.1, round(p.2, 3)), naiveBayesClassifierWithAllProbs('nb_nullable_dict', t)) AS all_probs
FROM (SELECT arrayJoin(CAST(['good great', NULL, 'bad awful'] AS Array(Nullable(String)))) AS t)
ORDER BY t NULLS FIRST;

DROP DICTIONARY nb_nullable_dict;
DROP TABLE nb_nullable_src;
