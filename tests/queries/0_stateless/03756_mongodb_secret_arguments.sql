-- Tags: no-fasttest

SELECT * FROM mongodb(some_named_collection, now()); -- { serverError NAMED_COLLECTION_DOESNT_EXIST }
