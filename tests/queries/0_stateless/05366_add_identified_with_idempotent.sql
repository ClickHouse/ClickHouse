-- Repeating `ADD IDENTIFIED WITH` must not accumulate identical methods.

CREATE USER {CLICKHOUSE_DATABASE:Identifier} IDENTIFIED WITH plaintext_password BY 'p1';

ALTER USER {CLICKHOUSE_DATABASE:Identifier} ADD IDENTIFIED WITH ldap SERVER 'srv';
ALTER USER {CLICKHOUSE_DATABASE:Identifier} ADD IDENTIFIED WITH ldap SERVER 'srv';
SELECT auth_type, auth_params FROM system.users WHERE name = currentDatabase();

-- A method repeated within one statement is added once.
ALTER USER {CLICKHOUSE_DATABASE:Identifier} ADD IDENTIFIED WITH kerberos REALM 'r', kerberos REALM 'r';
SELECT auth_type, auth_params FROM system.users WHERE name = currentDatabase();

-- The same hash with a different salt is a different credential.
ALTER USER {CLICKHOUSE_DATABASE:Identifier} ADD IDENTIFIED WITH sha256_hash BY 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855' SALT 'salt1';
ALTER USER {CLICKHOUSE_DATABASE:Identifier} ADD IDENTIFIED WITH sha256_hash BY 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855' SALT 'salt2';
SELECT auth_type FROM system.users WHERE name = currentDatabase();

DROP USER {CLICKHOUSE_DATABASE:Identifier};

-- Deduplicate after applying the user-level deadline.
CREATE USER {CLICKHOUSE_DATABASE:Identifier} IDENTIFIED WITH plaintext_password BY 'p1';
ALTER USER {CLICKHOUSE_DATABASE:Identifier} VALID UNTIL '2099-01-01 00:00:00 UTC' ADD IDENTIFIED WITH ldap SERVER 'srv';
ALTER USER {CLICKHOUSE_DATABASE:Identifier} VALID UNTIL '2099-01-01 00:00:00 UTC' ADD IDENTIFIED WITH ldap SERVER 'srv';
SELECT auth_type, arrayMap(x -> toUInt32(x), valid_until) FROM system.users WHERE name = currentDatabase();

DROP USER {CLICKHOUSE_DATABASE:Identifier};

-- `RESET AUTHENTICATION METHODS TO NEW` keeps the method added last, even if the user already had it.
CREATE USER {CLICKHOUSE_DATABASE:Identifier} IDENTIFIED WITH plaintext_password BY 'p1', ldap SERVER 'srv';
ALTER USER {CLICKHOUSE_DATABASE:Identifier} ADD IDENTIFIED WITH plaintext_password BY 'p1';
ALTER USER {CLICKHOUSE_DATABASE:Identifier} RESET AUTHENTICATION METHODS TO NEW;
SELECT auth_type FROM system.users WHERE name = currentDatabase();

DROP USER {CLICKHOUSE_DATABASE:Identifier};
