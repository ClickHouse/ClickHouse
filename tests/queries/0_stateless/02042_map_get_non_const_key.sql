SELECT map[key]
FROM
(
    SELECT materialize('key') AS key,  CAST((['key'], ['value']), 'Map(String, String)') AS map
);
