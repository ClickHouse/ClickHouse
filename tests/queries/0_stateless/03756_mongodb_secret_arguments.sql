-- Tags: no-fasttest

SELECT * FROM mongodb(some_named_collection, now()); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }