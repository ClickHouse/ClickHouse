-- This query used to reproduce a `CollectSetsVisitor` exception on an unresolved IN-family
-- function in a table function argument. Unresolved arguments are no longer traversed,
-- and the query is now rejected because '' is not a valid optimization name.
DROP TABLE IF EXISTS data;
CREATE TABLE data (date Date, c0 UInt64) ENGINE = MergeTree ORDER BY date;

SELECT * FROM mergeTreeAnalyzeIndexes(currentDatabase(), data, 1 = 1, [], '', globalNotIn(unknown_col)); -- { serverError BAD_ARGUMENTS }

DROP TABLE data;
