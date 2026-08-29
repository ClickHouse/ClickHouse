-- Tests that `PostingList` (the internal data type backing projection text index columns)
-- is rejected at user-facing DDL — its serialization requires a projection_index_context
-- that only exists on the projection-internal read/write paths. Allowing it as a regular
-- table column would let the table be created and then fail at INSERT/SELECT.
--
-- We only assert the top-level case here. Nested forms (Array/Nullable/Tuple) are also
-- rejected, but by different mechanisms (serialization hash construction) that emit
-- LOGICAL_ERROR plus a stack trace to client stderr, which trips the test runner's
-- "having stderror" check. The top-level rejection by `checkAllTypesAreAllowedInTable`
-- is the user-facing guarantee we need.

SET allow_experimental_projection_text_index = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab (c PostingList(0)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError DATA_TYPE_CANNOT_BE_USED_IN_TABLES }

CREATE TABLE tab (id UInt32, str String) ENGINE = MergeTree ORDER BY tuple();
ALTER TABLE tab ADD COLUMN c PostingList(0); -- { serverError DATA_TYPE_CANNOT_BE_USED_IN_TABLES }
DROP TABLE tab;
