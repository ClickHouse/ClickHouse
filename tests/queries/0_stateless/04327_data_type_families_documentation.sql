-- Data types expose embedded documentation via system.data_type_families.

-- Representative data types must have a non-empty description and syntax.
SELECT name, length(description) > 0 AS has_description, length(syntax) > 0 AS has_syntax
FROM system.data_type_families
WHERE name IN ('Int32', 'Float64', 'Decimal', 'String', 'Array', 'UUID', 'JSON')
ORDER BY name;

-- The Array documentation is migrated in full, so it mentions the type.
SELECT position(description, 'Array') > 0
FROM system.data_type_families
WHERE name = 'Array';

-- Related types are exposed as an array.
SELECT related
FROM system.data_type_families
WHERE name = 'Int32';
