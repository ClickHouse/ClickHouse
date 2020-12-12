SELECT 'Array min ', (arrayMin(array(1,2,3,4,5,6)));
SELECT 'Array max ', (arrayMax(array(1,2,3,4,5,6)));
SELECT 'Array sum ', (arraySum(array(1,2,3,4,5,6)));
SELECT 'Array avg ', (arrayAvg(array(1,2,3,4,5,6)));

DROP TABLE IF EXISTS test_aggregation;
CREATE TABLE test_aggregation (x Array(Int)) ENGINE=TinyLog;

INSERT INTO test_aggregation VALUES ([1,2,3,4,5,6]), ([]), ([1,2,3]);

SELECT 'Table array int min';
SELECT arrayMin(x) FROM test_aggregation;
SELECT 'Table array int max';
SELECT arrayMax(x) FROM test_aggregation;
SELECT 'Table array int sum';
SELECT arraySum(x) FROM test_aggregation;
SELECT 'Table array int avg';
SELECT arrayAvg(x) FROM test_aggregation;

DROP TABLE test_aggregation;

CREATE TABLE test_aggregation (x Array(Decimal64(8))) ENGINE=TinyLog;

INSERT INTO test_aggregation VALUES ([1,2,3,4,5,6]), ([]), ([1,2,3]);

SELECT 'Table array decimal min';
SELECT arrayMin(x) FROM test_aggregation;
SELECT 'Table array decimal max';
SELECT arrayMax(x) FROM test_aggregation;
SELECT 'Table array decimal sum';
SELECT arraySum(x) FROM test_aggregation;
SELECT 'Table array decimal avg';
SELECT arrayAvg(x) FROM test_aggregation;

DROP TABLE test_aggregation;