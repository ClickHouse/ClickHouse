-- Tags: no-random-merge-tree-settings

-- Test for ambiguity between Nullable's "null" subcolumn and a Tuple element named "null".
-- The column `t` is Nullable(Tuple(null UInt32)), so `t.null` could mean either
-- the null-map of the Nullable or the "null" element of the Tuple.
-- This should not cause an exception.

SET allow_experimental_nullable_tuple_type = 1;

DROP TABLE IF EXISTS t_nullable_tuple_null;

CREATE TABLE t_nullable_tuple_null (t Nullable(Tuple(`null` UInt32))) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_nullable_tuple_null VALUES ((10)), ((20));

SELECT t IS NULL, t.`null` FROM t_nullable_tuple_null ORDER BY t.`null`;

DROP TABLE t_nullable_tuple_null;
