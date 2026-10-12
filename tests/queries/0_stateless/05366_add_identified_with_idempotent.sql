-- Repeating `ADD IDENTIFIED WITH` must not accumulate identical methods.

CREATE USER {CLICKHOUSE_DATABASE:Identifier} IDENTIFIED WITH plaintext_password BY 'p1';

ALTER USER {CLICKHOUSE_DATABASE:Identifier} ADD IDENTIFIED WITH ldap SERVER 'srv';
ALTER USER {CLICKHOUSE_DATABASE:Identifier} ADD IDENTIFIED WITH ldap SERVER 'srv';
SELECT auth_type, auth_params FROM system.users WHERE name = currentDatabase();

-- A method the user already has, and a method repeated within one statement, are added once.
ALTER USER {CLICKHOUSE_DATABASE:Identifier} ADD IDENTIFIED WITH plaintext_password BY 'p1', kerberos REALM 'r', kerberos REALM 'r';
SELECT auth_type, auth_params FROM system.users WHERE name = currentDatabase();

-- A method differing in any parameter, including its own `VALID UNTIL`, is a separate one.
ALTER USER {CLICKHOUSE_DATABASE:Identifier} ADD IDENTIFIED WITH ldap SERVER 'srv2';
ALTER USER {CLICKHOUSE_DATABASE:Identifier} ADD IDENTIFIED WITH ldap SERVER 'srv' VALID UNTIL '2100-01-01 00:00:00 UTC';
ALTER USER {CLICKHOUSE_DATABASE:Identifier} ADD IDENTIFIED WITH ldap SERVER 'srv' VALID UNTIL '2100-01-01 00:00:00 UTC';
SELECT auth_type, auth_params, arrayMap(x -> toUInt32(x), valid_until) FROM system.users WHERE name = currentDatabase();

-- The same hash with a different salt is a different credential.
ALTER USER {CLICKHOUSE_DATABASE:Identifier} ADD IDENTIFIED WITH sha256_hash BY 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855' SALT 'salt1';
ALTER USER {CLICKHOUSE_DATABASE:Identifier} ADD IDENTIFIED WITH sha256_hash BY 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855' SALT 'salt2';
ALTER USER {CLICKHOUSE_DATABASE:Identifier} ADD IDENTIFIED WITH sha256_hash BY 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855' SALT 'salt1';
SELECT auth_type FROM system.users WHERE name = currentDatabase();

DROP USER {CLICKHOUSE_DATABASE:Identifier};

-- Deduplicate after applying the user-level deadline.
CREATE USER {CLICKHOUSE_DATABASE:Identifier} IDENTIFIED WITH plaintext_password BY 'p1';
ALTER USER {CLICKHOUSE_DATABASE:Identifier} VALID UNTIL '2099-01-01 00:00:00 UTC' ADD IDENTIFIED WITH ldap SERVER 'srv';
ALTER USER {CLICKHOUSE_DATABASE:Identifier} VALID UNTIL '2099-01-01 00:00:00 UTC' ADD IDENTIFIED WITH ldap SERVER 'srv';
SELECT auth_type, arrayMap(x -> toUInt32(x), valid_until) FROM system.users WHERE name = currentDatabase();

DROP USER {CLICKHOUSE_DATABASE:Identifier};
