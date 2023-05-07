set optimize_distinct_in_order=1;

DROP TABLE IF EXISTS test;

CREATE TABLE test
(
    `c1` String,
    `c2` String
)
ENGINE = MergeTree
ORDER BY c1;

INSERT INTO test(c1, c2) VALUES ('1',  ''), ('2', '');

SELECT DISTINCT c2, c1 FROM test FORMAT TSV;
