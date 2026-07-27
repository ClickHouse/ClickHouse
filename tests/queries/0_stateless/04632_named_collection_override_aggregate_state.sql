-- Tags: no-fasttest, no-replicated-database
-- no-fasttest: the s3 table function is not available in the fasttest build.
-- no-replicated-database: CREATE NAMED COLLECTION is not replicated.

-- A named collection key override is materialized as text with fieldToString(), which has no
-- text representation for an aggregate state and raised a LOGICAL_ERROR (an abort under debug
-- and sanitizer builds). The value comes from the query, so it must be a plain user error.
-- Both override paths go through the same helper (getKeyValueFromASTImpl): the table-function
-- one (tryGetNamedCollectionWithOverrides) and the BACKUP one (getParamsMapFromAST).
-- Only a top-level aggregate state is affected; nested in a container it is formatted by
-- FieldVisitorToString, which does not throw.

DROP NAMED COLLECTION IF EXISTS c_04632;
CREATE NAMED COLLECTION c_04632 AS url = 'http://localhost:1/x', format = 'TSV';

-- Rejected while parsing the table-function arguments, so no request is ever made.
SELECT * FROM s3(c_04632, format = initializeAggregation('anyState', 'TSV')); -- { serverError BAD_ARGUMENTS }
SELECT * FROM s3(c_04632, url = initializeAggregation('uniqState', 1)); -- { serverError BAD_ARGUMENTS }

BACKUP TABLE system.one TO S3(c_04632, url = initializeAggregation('anyState', 'v')); -- { serverError BAD_ARGUMENTS }

DROP NAMED COLLECTION c_04632;

SELECT 'ok';
