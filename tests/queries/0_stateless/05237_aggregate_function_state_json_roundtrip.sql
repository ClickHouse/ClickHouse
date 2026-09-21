-- An `AggregateFunction` column written by a JSON output format is read back by a JSON input format.

INSERT INTO FUNCTION file(currentDatabase() || '_05237.jsonl', JSONEachRow, 'k UInt8, s AggregateFunction(sum, UInt64), m AggregateFunction(max, String), g AggregateFunction(groupArray, UInt32)')
SELECT 1, sumState(number), maxState(toString(number)), groupArrayState(toUInt32(number)) FROM numbers(10) SETTINGS engine_file_truncate_on_insert = 1;

SELECT k, sumMerge(s), maxMerge(m), groupArrayMerge(g)
FROM file(currentDatabase() || '_05237.jsonl', JSONEachRow, 'k UInt8, s AggregateFunction(sum, UInt64), m AggregateFunction(max, String), g AggregateFunction(groupArray, UInt32)')
GROUP BY k ORDER BY k
SETTINGS aggregate_function_input_format = 'state';
