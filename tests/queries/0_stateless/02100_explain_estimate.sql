-- Tags: no-random-merge-tree-settings
-- no-random-merge-tree-settings: impacts index scoring

CREATE TABLE test_explain_estimate (
    id UInt32,
    name UInt32
) ENGINE = MergeTree() ORDER BY id;

INSERT INTO test_explain_estimate
SELECT
    number as id,
    number as name
FROM numbers(40000);

CREATE TABLE test_explain_estimate_1 (
    id UInt32,
    name UInt32
) ENGINE = MergeTree() ORDER BY id;

INSERT INTO test_explain_estimate_1
SELECT
    number as id,
    number as name
FROM numbers(40000);

ADVISE INDEX (SELECT * FROM test_explain_estimate JOIN test_explain_estimate_1 ON test_explain_estimate.name = test_explain_estimate_1.name WHERE test_explain_estimate.name = 156);

DROP TABLE IF EXISTS test_explain_estimate; 
DROP TABLE IF EXISTS test_explain_estimate_1; 