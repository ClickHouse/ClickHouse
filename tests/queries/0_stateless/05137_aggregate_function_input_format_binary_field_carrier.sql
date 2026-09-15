-- The `Field` binary form of an `AggregateFunction` is the length-prefixed opaque state, independently of
-- `aggregate_function_input_format`: it is only ever read back from what `serializeBinary(Field, ...)` wrote,
-- so it has to stay its exact inverse. A `JSON` path that does not fit into the dynamic paths goes to the
-- shared data through that form, and reading its length prefix as an argument value would corrupt both the
-- value and everything that follows it in the row.

SELECT 'JSON shared data carrier';

SELECT count(), any(JSONAllPathsWithTypes(j)), any(hex(CAST(j.a, 'String')))
FROM format(RowBinary, 'j JSON(max_dynamic_paths=0)', unhex('010161250003616e79000104' || '09' || '010100000000000000'))
SETTINGS aggregate_function_input_format = 'state';

SELECT count(), any(JSONAllPathsWithTypes(j)), any(hex(CAST(j.a, 'String')))
FROM format(RowBinary, 'j JSON(max_dynamic_paths=0)', unhex('010161250003616e79000104' || '09' || '010100000000000000'))
SETTINGS aggregate_function_input_format = 'value';

SELECT count(), any(JSONAllPathsWithTypes(j)), any(hex(CAST(j.a, 'String')))
FROM format(RowBinary, 'j JSON(max_dynamic_paths=0)', unhex('010161250003616e79000104' || '09' || '010100000000000000'))
SETTINGS aggregate_function_input_format = 'array';

-- The value form belongs to the `IColumn` path, which is what a binary input format reads a column with,
-- including a column nested inside another one.

SELECT 'Value form on the column path';

SELECT finalizeAggregation(tupleElement(t, 1))
FROM format(RowBinary, 't Tuple(AggregateFunction(any, UInt64))', unhex('2a00000000000000'))
SETTINGS aggregate_function_input_format = 'value';

SELECT dynamicType(d), finalizeAggregation(CAST(d, 'AggregateFunction(any, UInt64)'))
FROM format(RowBinary, 'd Dynamic', unhex('250003616e79000104' || '2a00000000000000'))
SETTINGS aggregate_function_input_format = 'value';

SELECT finalizeAggregation(tupleElement(t, 1))
FROM format(RowBinary, 't Tuple(AggregateFunction(any, UInt64))', unhex('012a00000000000000'))
SETTINGS aggregate_function_input_format = 'array';
