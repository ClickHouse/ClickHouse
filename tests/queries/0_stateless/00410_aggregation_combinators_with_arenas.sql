SET allow_deprecated_error_prone_window_functions = 1;
DROP TABLE IF EXISTS arena;
CREATE TABLE arena (k UInt8, d String) ENGINE = Memory;
INSERT INTO arena SELECT number % 10 AS k, hex(intDiv(number, 10) % 1000) AS d FROM system.numbers LIMIT 10000000;
SELECT length(groupUniqArrayIf(d, d != hex(0))) FROM arena GROUP BY k;
SELECT length(groupUniqArrayMerge(ds)) FROM (SELECT k, groupUniqArrayState(d) AS ds FROM arena GROUP BY k) GROUP BY k;
DROP TABLE IF EXISTS arena;

SELECT length(arrayReduce('groupUniqArray', [[1, 2], [1],  emptyArrayUInt8(), [1], [1, 2]]));
SELECT min(x), max(x) FROM (SELECT length(arrayReduce('groupUniqArray', [hex(number), hex(number+1), hex(number)])) AS x FROM system.numbers LIMIT 100000);

-- Disable external aggregation because the state is reset for each new block of data in 'runningAccumulate' function.
SET max_bytes_before_external_group_by = 0;

SELECT sum(length(runningAccumulate(x))) FROM (SELECT groupUniqArrayState(toString(number % 10)) AS x, number FROM (SELECT * FROM system.numbers LIMIT 11) GROUP BY number ORDER BY number);
