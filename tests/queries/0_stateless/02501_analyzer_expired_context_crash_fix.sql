SET enable_analyzer = 1;

DROP TABLE IF EXISTS test_table;
CREATE TABLE test_table
(
    b Int64,
    a Int64,
    grp_aggreg AggregateFunction(groupArrayArray, Array(UInt64))
) ENGINE = MergeTree() ORDER BY a;

INSERT INTO test_table SELECT 0, 0, groupArrayArrayState([toUInt64(1)]);

SELECT b, a, JSONLength(grp_aggreg, 100, NULL) FROM test_table SETTINGS optimize_aggregation_in_order = 1;

DROP TABLE test_table;
