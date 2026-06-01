-- Disk types expose embedded documentation via system.disk_types.

-- Core disk types must have a non-empty description and syntax.
-- Only check disk types that are always present regardless of build options
-- (`encrypted` requires `USE_SSL`, so it is excluded here).
SELECT name, length(description) > 0 AS has_description, length(syntax) > 0 AS has_syntax
FROM system.disk_types
WHERE name IN ('local', 'object_storage', 'cache')
ORDER BY name;

-- Related disk types are exposed as an array.
SELECT length(related) > 0
FROM system.disk_types
WHERE name = 'local';
