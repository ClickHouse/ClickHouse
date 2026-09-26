-- Accessing an array element with INT_MIN index should not cause undefined behavior
-- (negation of -2147483648 overflows signed int).
-- https://s3.amazonaws.com/clickhouse-test-reports/json.html?REF=master&sha=36a049a7f14c6fbc7e0eb33dc32ec94514ceaa7c&name_0=MasterCI&name_1=AST%20fuzzer%20%28amd_ubsan%29

SELECT [1, 2, 3][toInt32(-2147483648)];
SELECT [(1, 'a'), (2, 'b')][toInt32(-2147483648)];
SELECT materialize([(NULL, 'x', 'y')]) AS t, (t[toInt32(-2147483648)]).1;
