--
SELECT 'Invocation with Date columns';

DROP TABLE IF EXISTS runningConcurrency_test;
CREATE TABLE runningConcurrency_test(begin Date, end Date) ENGINE = Memory;

INSERT INTO runningConcurrency_test VALUES ('2020-12-01', '2020-12-10'), ('2020-12-02', '2020-12-10'), ('2020-12-03', '2020-12-12'), ('2020-12-10', '2020-12-12'), ('2020-12-13', '2020-12-20');
SELECT runningConcurrency(begin, end) FROM runningConcurrency_test;

DROP TABLE runningConcurrency_test;

--
SELECT 'Invocation with DateTime';

DROP TABLE IF EXISTS runningConcurrency_test;
CREATE TABLE runningConcurrency_test(begin DateTime, end DateTime) ENGINE = Memory;

INSERT INTO runningConcurrency_test VALUES ('2020-12-01 00:00:00', '2020-12-01 00:59:59'), ('2020-12-01 00:30:00', '2020-12-01 00:59:59'), ('2020-12-01 00:40:00', '2020-12-01 01:30:30'), ('2020-12-01 01:10:00', '2020-12-01 01:30:30'), ('2020-12-01 01:50:00', '2020-12-01 01:59:59');
SELECT runningConcurrency(begin, end) FROM runningConcurrency_test;

DROP TABLE runningConcurrency_test;

--
SELECT 'Invocation with DateTime64';

DROP TABLE IF EXISTS runningConcurrency_test;
CREATE TABLE runningConcurrency_test(begin DateTime64(3), end DateTime64(3)) ENGINE = Memory;

INSERT INTO runningConcurrency_test VALUES ('2020-12-01 00:00:00.000', '2020-12-01 00:00:00.100'), ('2020-12-01 00:00:00.010', '2020-12-01 00:00:00.100'), ('2020-12-01 00:00:00.020', '2020-12-01 00:00:00.200'), ('2020-12-01 00:00:00.150', '2020-12-01 00:00:00.200'), ('2020-12-01 00:00:00.250', '2020-12-01 00:00:00.300');
SELECT runningConcurrency(begin, end) FROM runningConcurrency_test;

DROP TABLE runningConcurrency_test;

--
SELECT 'Erroneous cases';

-- Constant columns are currently not supported.
SELECT runningConcurrency(toDate(arrayJoin([1, 2])), toDate('2000-01-01')); -- { serverError ILLEGAL_COLUMN }

-- Unsupported data types
SELECT runningConcurrency('strings are', 'not supported'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT runningConcurrency(NULL, NULL); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT runningConcurrency(CAST(NULL, 'Nullable(DateTime)'), CAST(NULL, 'Nullable(DateTime)')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Mismatching data types
SELECT runningConcurrency(toDate('2000-01-01'), toDateTime('2000-01-01 00:00:00')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- begin > end
SELECT runningConcurrency(toDate('2000-01-02'), toDate('2000-01-01')); -- { serverError INCORRECT_DATA }

                                                       
