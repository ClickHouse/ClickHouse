set allow_experimental_dynamic_type=1;
set cast_keep_nullable=1;
SELECT toFixedString('str', 3), 3, CAST(if(1 = 0, toInt8(3), NULL), 'Int32') AS x from numbers(10) GROUP BY GROUPING SETS ((CAST(toInt32(1), 'Int32')), ('str', 3), (CAST(toFixedString('str', 3), 'Dynamic')), (CAST(toFixedString(toFixedString('str', 3), 3), 'Dynamic')));

