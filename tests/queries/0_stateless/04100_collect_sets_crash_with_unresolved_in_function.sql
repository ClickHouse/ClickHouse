-- This query used to reproduce a `CollectSetsVisitor` exception on an unresolved IN-family
-- function in a table function argument. Unresolved arguments are no longer traversed,
-- and the query is now rejected because '' is not a valid optimization name.
-- With the old analyzer the malformed `globalNotIn` is rejected earlier by `ActionsVisitor`
-- with `NUMBER_OF_ARGUMENTS_DOESNT_MATCH`; with the analyzer the unknown optimization name
-- is rejected with `BAD_ARGUMENTS`.
DROP TABLE IF EXISTS data;
CREATE TABLE data (date Date, c0 UInt64) ENGINE = MergeTree ORDER BY date;

SELECT * FROM mergeTreeAnalyzeIndexes(currentDatabase(), data, 1 = 1, [], '', globalNotIn(unknown_col)); -- { serverError BAD_ARGUMENTS, NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

DROP TABLE data;
