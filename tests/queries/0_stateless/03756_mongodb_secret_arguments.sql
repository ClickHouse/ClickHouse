-- Tags: no-fasttest

SELECT * FROM mongodb(some_named_collection, now()); -- { serverError BAD_ARGUMENTS }
