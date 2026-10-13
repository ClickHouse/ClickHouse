-- A failed attempt to insert a nested JSON object must not leave stale bytes in its shared data.
-- The cast to `Dynamic(max_types=0)` serializes the nested object in binary form.
SELECT toString(CAST(p.entries, 'Dynamic(max_types=0)'))
FROM format(JSONAsObject, 'p JSON(max_dynamic_paths=4)', '{"entries":[{}]}\n{"entries":[{"a":1,"b":0,"c":[true,1]}]}');
