SELECT min_by(value, key), max_by(value, key)
FROM VALUES('value String, key UInt8', ('a', 3), ('b', 1), ('c', 2));

SELECT MIN_BY(value, key), MAX_BY(value, key)
FROM VALUES('value String, key UInt8', ('a', 3), ('b', 1), ('c', 2));

SELECT min_byIf(value, key, key != 2), max_byIf(value, key, key != 2)
FROM VALUES('value String, key UInt8', ('a', 3), ('b', 1), ('c', 2));

SELECT name, alias_to, is_aggregate, case_insensitive
FROM system.functions
WHERE name IN ('min_by', 'max_by')
ORDER BY name;
