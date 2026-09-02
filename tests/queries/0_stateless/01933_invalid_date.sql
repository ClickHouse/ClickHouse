SELECT toDate('07-08-2019'); -- { serverError CANNOT_PARSE_DATE }
SELECT toDate('2019-0708'); -- { serverError CANNOT_PARSE_DATE }
SELECT toDate('201907-08'); -- { serverError CANNOT_PARSE_DATE }
SELECT toDate('2019^7^8');

CREATE TEMPORARY TABLE test (d Date);
INSERT INTO test VALUES ('2018-01-01');

SELECT * FROM test WHERE d >= '07-08-2019'; -- { serverError CANNOT_PARSE_DATE }
SELECT * FROM test WHERE d >= '2019-07-08';
