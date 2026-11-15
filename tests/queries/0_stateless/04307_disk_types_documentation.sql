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

-- Every registered disk type (including optional ones compiled into this build,
-- such as `s3`, `hdfs`, or `azure_blob_storage`) must expose a non-empty
-- description and syntax. This assertion is independent of build options.
SELECT name
FROM system.disk_types
WHERE length(description) = 0 OR length(syntax) = 0
ORDER BY name;
