DROP TABLE IF EXISTS test_serialization;

CREATE TABLE test_serialization
(
    id UInt64,
    text AggregateFunction(groupConcat, String)
) ENGINE = AggregatingMergeTree() ORDER BY id;

INSERT INTO test_serialization SELECT
    1,
    groupConcatState('First');

SELECT groupConcatMerge(text) AS concatenated_text FROM test_serialization GROUP BY id;

INSERT INTO test_serialization SELECT
    2,
    groupConcatState('Second');

SELECT groupConcatMerge(text) AS concatenated_text FROM test_serialization GROUP BY id ORDER BY id;

DROP TABLE IF EXISTS test_serialization;


