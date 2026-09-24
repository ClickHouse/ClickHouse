-- The *_PRIVILEGES views show only positive, non-wildcard, non-parameterized grants:
-- partial revokes, wildcard grants (e.g. `GRANT SELECT ON db_prefix*.*`) and parameterized grants
-- (e.g. `GRANT TABLE ENGINE ON TinyLog`, `GRANT READ ON S3`) cannot be represented in the
-- MySQL-compatible format and are omitted. `system.grants` is the authoritative source.
-- The grantee (a user or a role - ClickHouse roles have no MySQL analog) is rendered
-- MySQL-style as 'name'@'%', with backslashes and quotes in names escaped as \\ and \'.
ATTACH VIEW user_privileges
    (
     `grantee` String,
     `table_catalog` String,
     `privilege_type` String,
     `is_grantable` String,
     `GRANTEE` String,
     `TABLE_CATALOG` String,
     `PRIVILEGE_TYPE` String,
     `IS_GRANTABLE` String
)
SQL SECURITY INVOKER
AS SELECT
    concat('\'', replaceAll(replaceAll(ifNull(user_name, role_name), '\\', '\\\\'), '\'', '\\\''), '\'@\'%\'') AS grantee,
    'def'                         AS table_catalog,
    CAST(access_type, 'String')   AS privilege_type,
    if(grant_option, 'YES', 'NO') AS is_grantable,
    grantee                       AS GRANTEE,
    table_catalog                 AS TABLE_CATALOG,
    privilege_type                AS PRIVILEGE_TYPE,
    is_grantable                  AS IS_GRANTABLE
FROM system.grants
WHERE (database IS NULL)
    AND (access_object = '')
    AND (is_wildcard = 0)
    AND (is_partial_revoke = 0);
