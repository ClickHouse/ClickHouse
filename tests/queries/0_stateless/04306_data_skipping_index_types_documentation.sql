-- Data skipping index types expose embedded documentation via system.data_skipping_index_types.

-- Core index types must have a non-empty description and syntax.
SELECT name, length(description) > 0 AS has_description, length(syntax) > 0 AS has_syntax
FROM system.data_skipping_index_types
WHERE name IN ('minmax', 'set', 'bloom_filter', 'tokenbf_v1', 'text')
ORDER BY name;

-- Related index types are exposed as an array.
SELECT related
FROM system.data_skipping_index_types
WHERE name = 'minmax';
