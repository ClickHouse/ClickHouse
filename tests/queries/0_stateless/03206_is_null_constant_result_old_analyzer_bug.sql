CREATE TABLE left (x UUID) ORDER BY tuple();

CREATE TABLE right (x UUID) ORDER BY tuple();

set allow_experimental_analyzer=0;

SELECT left.x, (right.x IS NULL)::Boolean FROM left LEFT OUTER JOIN right ON left.x = right.x GROUP BY ALL;

SELECT isNullable(number)::Boolean, now() FROM numbers(2) GROUP BY isNullable(number)::Boolean, now() FORMAT Null;

SELECT isNull(number)::Boolean, now() FROM numbers(2) GROUP BY isNull(number)::Boolean, now() FORMAT Null;

SELECT (number IS NULL)::Boolean, now() FROM numbers(2) GROUP BY (number IS NULL)::Boolean, now() FORMAT Null;

set allow_experimental_analyzer=1;

SELECT left.x, (right.x IS NULL)::Boolean FROM left LEFT OUTER JOIN right ON left.x = right.x GROUP BY ALL;

SELECT isNullable(number)::Boolean, now() FROM numbers(2) GROUP BY isNullable(number)::Boolean, now() FORMAT Null;

SELECT isNull(number)::Boolean, now() FROM numbers(2) GROUP BY isNull(number)::Boolean, now() FORMAT Null;

SELECT (number IS NULL)::Boolean, now() FROM numbers(2) GROUP BY (number IS NULL)::Boolean, now() FORMAT Null;
