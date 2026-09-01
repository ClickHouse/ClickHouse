-- Reproducer for CollectSetsVisitor crash when an unresolved IN-family function
-- with wrong number of arguments ends up in a table function's non-skipped argument.
DROP TABLE IF EXISTS data;
CREATE TABLE data (date Date, c0 UInt64) ENGINE = MergeTree ORDER BY date;

SELECT * FROM mergeTreeAnalyzeIndexes(currentDatabase(), data, 1 = 1, [], '', globalNotIn(unknown_col)); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

DROP TABLE data;
