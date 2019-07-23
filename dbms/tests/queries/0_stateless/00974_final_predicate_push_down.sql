DROP TABLE IF EXISTS test_00974;

CREATE TABLE test_00974
(
    date Date,
    x Int32,
    ver UInt64
)
ENGINE = ReplacingMergeTree(date, x, 4096);

INSERT INTO test_00974 VALUES ('2019-07-23', 1, 1), ('2019-07-23', 1, 2);
INSERT INTO test_00974 VALUES ('2019-07-23', 2, 1), ('2019-07-23', 2, 2);

SELECT COUNT() FROM (SELECT * FROM test_00974 FINAL) where x = 1 SETTINGS allow_push_predicate_to_final_subquery = 0  FORMAT JSON ;
SELECT COUNT() FROM (SELECT * FROM test_00974 FINAL) where x = 1 SETTINGS allow_push_predicate_to_final_subquery = 1  FORMAT JSON ;

DROP TABLE test_00974;
