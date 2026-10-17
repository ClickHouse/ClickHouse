-- Tags: no-fasttest, no-random-settings

SET enable_json_type = 1;

-- Objects inside arrays get the plain inferred JSON type, whatever the column type declares.
SELECT 'shared regexp', dynamicType(j.arr), j.arr.:`Array(JSON)`.a, j.arr, JSONSharedDataPaths(j)
FROM format(JSONEachRow, 'j JSON(SHARED REGEXP \'^arr\')', '{"j":{"arr":[{"a":1,"b":2,"x":3,"s":"q"}]}}');
