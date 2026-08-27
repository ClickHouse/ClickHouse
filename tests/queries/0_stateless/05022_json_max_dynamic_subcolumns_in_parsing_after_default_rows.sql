-- `max_dynamic_subcolumns_in_json_type_parsing` is applied to the column the values are parsed into,
-- so rows that reach it before the first object (a row with the field absent) must not stop it.

SELECT JSONDynamicPaths(json), JSONSharedDataPaths(json)
FROM format(JSONEachRow, 'json JSON', $$
{}
{"json":{"a":1,"b":2,"c":3,"d":4,"e":5,"f":6}}
$$)
SETTINGS max_dynamic_subcolumns_in_json_type_parsing = 1;

-- The same for a typed path, whose column receives a default while the enclosing object is parsed.
SELECT JSONDynamicPaths(json.a), JSONSharedDataPaths(json.a)
FROM format(JSONEachRow, 'json JSON(a JSON)', $$
{"json":{}}
{"json":{"a":{"x":1,"y":2,"z":3}}}
$$)
SETTINGS max_dynamic_subcolumns_in_json_type_parsing = 1;
