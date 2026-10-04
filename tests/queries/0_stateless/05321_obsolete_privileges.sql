-- Obsolete privileges are kept for compatibility and are marked as such in system.privileges and system.grants.

SELECT privilege, is_obsolete FROM system.privileges WHERE is_obsolete ORDER BY privilege;

DROP USER IF EXISTS {CLICKHOUSE_DATABASE:Identifier};
CREATE USER {CLICKHOUSE_DATABASE:Identifier};

-- An obsolete privilege can still be granted and revoked.
GRANT SYSTEM RELOAD MODEL ON *.* TO {CLICKHOUSE_DATABASE:Identifier};
GRANT SELECT ON *.* TO {CLICKHOUSE_DATABASE:Identifier};
SELECT access_type, is_obsolete FROM system.grants WHERE user_name = currentDatabase() ORDER BY access_type;

REVOKE SYSTEM RELOAD MODEL ON *.* FROM {CLICKHOUSE_DATABASE:Identifier};
SELECT access_type, is_obsolete FROM system.grants WHERE user_name = currentDatabase() ORDER BY access_type;

DROP USER {CLICKHOUSE_DATABASE:Identifier};
