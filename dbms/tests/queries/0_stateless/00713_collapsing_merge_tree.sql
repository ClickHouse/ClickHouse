DROP TABLE IF EXISTS test.collapsing;

CREATE TABLE test.collapsing(key String, value String, sign Int8) ENGINE CollapsingMergeTree(sign)
    ORDER BY key
    SETTINGS enable_vertical_merge_algorithm=1,
             vertical_merge_algorithm_min_rows_to_activate=0,
             vertical_merge_algorithm_min_columns_to_activate=0;

INSERT INTO test.collapsing VALUES ('k1', 'k1v1', 1);
INSERT INTO test.collapsing VALUES ('k1', 'k1v1', -1), ('k1', 'k1v2', 1);
INSERT INTO test.collapsing VALUES ('k2', 'k2v1', 1), ('k2', 'k2v1', -1), ('k3', 'k3v1', 1);
INSERT INTO test.collapsing VALUES ('k4', 'k4v1', -1), ('k4', 'k4v2', 1), ('k4', 'k4v2', -1);

OPTIMIZE TABLE test.collapsing PARTITION tuple() FINAL;

SELECT * FROM test.collapsing ORDER BY key;

DROP TABLE test.collapsing;
