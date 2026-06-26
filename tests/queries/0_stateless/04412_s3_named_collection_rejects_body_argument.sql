-- Tags: no-fasttest, no-replicated-database
-- Tag no-fasttest: the s3 table function is unavailable in fasttest builds.
-- Tag no-replicated-database: a named collection is a global object.

-- The `body(...)` argument is supported only by the `url` table function, not by S3. The positional
-- argument parser rejects it, but the named-collection path ignores non-`equals` function arguments
-- (with the default `allow_named_collection_override_by_default = 1`), so `s3(collection, body(...))`
-- used to be accepted while the body was silently dropped. It must be rejected loudly instead.
-- The error is thrown during argument parsing, so no connection to the URL is made.

DROP NAMED COLLECTION IF EXISTS named_coll_04412_s3_body;
CREATE NAMED COLLECTION named_coll_04412_s3_body AS url = 'http://localhost:11111/test/data.csv';

SET enable_analyzer = 1;
SELECT * FROM s3(named_coll_04412_s3_body, body('payload')); -- { serverError BAD_ARGUMENTS }
SELECT * FROM s3(named_coll_04412_s3_body, body((SELECT 1))); -- { serverError BAD_ARGUMENTS }
SELECT * FROM s3(named_coll_04412_s3_body, body('')); -- { serverError BAD_ARGUMENTS }

SET enable_analyzer = 0;
SELECT * FROM s3(named_coll_04412_s3_body, body('payload')); -- { serverError BAD_ARGUMENTS }
SELECT * FROM s3(named_coll_04412_s3_body, body((SELECT 1))); -- { serverError BAD_ARGUMENTS }
SELECT * FROM s3(named_coll_04412_s3_body, body('')); -- { serverError BAD_ARGUMENTS }

DROP NAMED COLLECTION named_coll_04412_s3_body;
