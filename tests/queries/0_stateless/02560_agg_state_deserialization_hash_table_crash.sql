DROP TABLE IF EXISTS tab;
create table tab (d Int64, s AggregateFunction(groupUniqArrayArray, Array(UInt64)), c SimpleAggregateFunction(groupUniqArrayArray, Array(UInt64))) engine = SummingMergeTree() order by d;
INSERT INTO tab VALUES (1, 'このコー'); -- { error TOO_LARGE_ARRAY_SIZE }
DROP TABLE tab;
