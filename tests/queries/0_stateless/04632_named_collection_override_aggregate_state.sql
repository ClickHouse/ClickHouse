-- Tags: no-fasttest, no-parallel, no-replicated-database
-- no-fasttest: the s3 table function is not available in the fasttest build.
-- no-parallel: CREATE/DROP NAMED COLLECTION mutate global server state shared by concurrent
-- tests (see 02918_fuzzjson_table_function.sql for the same requirement).
-- no-replicated-database: CREATE NAMED COLLECTION is not replicated.

-- A named collection key override is materialized as text with fieldToString(), which has no
-- text representation for an aggregate state and raised a LOGICAL_ERROR (an abort under debug
-- and sanitizer builds). The value comes from the query, so it must be a plain user error.
-- Both override paths go through the same helper (getKeyValueFromASTImpl): the table-function
-- one (tryGetNamedCollectionWithOverrides) and the BACKUP one (getParamsMapFromAST).
-- Only a top-level aggregate state is affected; nested in a container it is formatted by
-- FieldVisitorToString, which does not throw.

DROP NAMED COLLECTION IF EXISTS c_04632;
CREATE NAMED COLLECTION c_04632 AS url = 'http://localhost:1/bucket/key.tsv', format = 'TSV';

-- Rejected while parsing the table-function arguments, so no request is ever made.
SELECT * FROM s3(c_04632, format = initializeAggregation('anyState', 'TSV')); -- { serverError BAD_ARGUMENTS }
SELECT * FROM s3(c_04632, url = initializeAggregation('uniqState', 1)); -- { serverError BAD_ARGUMENTS }

BACKUP TABLE system.one TO S3(c_04632, url = initializeAggregation('anyState', 'v')); -- { serverError BAD_ARGUMENTS }

-- Nested in a container the state is reached through FieldVisitorToString (writeText for Array
-- delegates to it, and Tuple/Map go through writeFieldText), whose AggregateFunctionStateData
-- overload returns formatQuoted(data) instead of throwing. So these are stringified rather than
-- rejected; what matters is that they stay a clean user error and never a LOGICAL_ERROR. The
-- serialized state is not a format name, so the format check rejects it before any request.
SELECT * FROM s3(c_04632, format = [initializeAggregation('anyState', 'x')]); -- { serverError UNKNOWN_FORMAT }
SELECT * FROM s3(c_04632, format = [[initializeAggregation('anyState', 'x')]]); -- { serverError UNKNOWN_FORMAT }
SELECT * FROM s3(c_04632, format = tuple(initializeAggregation('anyState', 'x'), 1)); -- { serverError UNKNOWN_FORMAT }
SELECT * FROM s3(c_04632, format = map('k', initializeAggregation('anyState', 'x'))); -- { serverError UNKNOWN_FORMAT }
SELECT * FROM s3(c_04632, format = map(initializeAggregation('anyState', 'x'), 1)); -- { serverError UNKNOWN_FORMAT }

DROP NAMED COLLECTION c_04632;

SELECT 'ok';
