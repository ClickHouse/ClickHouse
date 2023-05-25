-- Tags: no-fasttest

SELECT h3GetRes0Indexes();
SELECT h3GetRes0Indexes(3); -- { serverError 42 }

SELECT h3GetRes0Indexes() FROM system.numbers LIMIT 5;
