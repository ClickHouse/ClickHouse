-- Tags: no-fasttest

SELECT h3GetRes0Indexes();
SELECT h3GetRes0Indexes(3); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

SELECT h3GetRes0Indexes() FROM system.numbers LIMIT 5;
