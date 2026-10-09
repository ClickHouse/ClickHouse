ATTACH VIEW collations
(
    collation_name String,
    COLLATION_NAME String
)
SQL SECURITY INVOKER
AS SELECT
    name AS collation_name,
    collation_name AS COLLATION_NAME
FROM system.collations
