-- Tags: no-parallel
CREATE FUNCTION _02098_alias_function AS x -> (((x * 2) AS x_doubled) + x_doubled);
SELECT _02098_alias_function(2);
DROP FUNCTION _02098_alias_function;
