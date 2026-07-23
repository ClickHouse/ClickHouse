-- Previously caused LOGICAL_ERROR: Bad cast from type DB::ColumnConst to DB::ColumnArray
SELECT kql_array_sort_desc([], [blockSerializedSize()]);
SELECT kql_array_sort_asc([], [blockSerializedSize()]);
