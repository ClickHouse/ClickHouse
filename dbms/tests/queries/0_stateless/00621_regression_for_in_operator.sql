DROP TABLE IF EXISTS test.regression_for_in_operator_view;
DROP TABLE IF EXISTS test.regression_for_in_operator;
CREATE TABLE test.regression_for_in_operator (d Date, v UInt32, g String) ENGINE=MergeTree(d, d, 8192);
CREATE MATERIALIZED VIEW test.regression_for_in_operator_view ENGINE=AggregatingMergeTree(d, (d,g), 8192) AS SELECT d, g, maxState(v) FROM test.regression_for_in_operator GROUP BY d, g;

INSERT INTO test.regression_for_in_operator SELECT today(), toString(number % 10), number FROM system.numbers limit 1000;

SELECT count() FROM test.regression_for_in_operator_view WHERE g = '5';
SELECT count() FROM test.regression_for_in_operator_view WHERE g IN ('5');
SELECT count() FROM test.regression_for_in_operator_view WHERE g IN ('5','6');

SET optimize_min_equality_disjunction_chain_length = 1;
SELECT count() FROM test.regression_for_in_operator_view WHERE g = '5' OR g = '6';

SET optimize_min_equality_disjunction_chain_length = 3;
SELECT count() FROM test.regression_for_in_operator_view WHERE g = '5' OR g = '6';

DROP TABLE test.regression_for_in_operator_view;
DROP TABLE test.regression_for_in_operator;
