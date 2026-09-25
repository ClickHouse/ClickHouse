-- Tags: no-fasttest, no-random-settings

SET enable_json_type = 1;

-- Objects inside arrays get the plain inferred JSON type, whatever the column type declares.
SELECT 'skip regexp', dynamicType(j.arr), j.arr.:`Array(JSON)`.a, j.arr
FROM format(JSONEachRow, 'j JSON(SKIP REGEXP \'b\')', '{"j":{"arr":[{"a":1,"b":2,"x":3,"s":"q"}]}}');

SELECT 'typed path', dynamicType(j.arr), j.arr.:`Array(JSON)`.a, j.arr
FROM format(JSONEachRow, 'j JSON(arr.x UInt64)', '{"j":{"arr":[{"a":1,"b":2,"x":3,"s":"q"}]}}');

SELECT 'skip path', dynamicType(j.arr), j.arr.:`Array(JSON)`.a, j.arr
FROM format(JSONEachRow, 'j JSON(SKIP arr.s)', '{"j":{"arr":[{"a":1,"b":2,"x":3,"s":"q"}]}}');

SELECT 'shared regexp', dynamicType(j.arr), j.arr.:`Array(JSON)`.a, j.arr, JSONSharedDataPaths(j)
FROM format(JSONEachRow, 'j JSON(SHARED REGEXP \'^arr\')', '{"j":{"arr":[{"a":1,"b":2,"x":3,"s":"q"}]}}');
