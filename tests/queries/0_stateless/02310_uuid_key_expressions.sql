-- Tests UUID behavior in MergeTree key expressions

SELECT 'Assert UUID primary key is not allowed by default on CREATE TABLE DDL';
CREATE TABLE users (
  uid UUID,
  name String,
  age Int16
)
ENGINE=MergeTree
ORDER BY (uid, name); -- { serverError ILLEGAL_COLUMN }

SELECT 'Assert UUID partition key is not allowed by default on CREATE TABLE DDL';
CREATE TABLE users_partition_uuid
(
    uid UUID,
    age Int16
)
ENGINE = MergeTree
ORDER BY age
PARTITION BY uid; -- { serverError ILLEGAL_COLUMN }

SELECT 'Assert explicit UUID PRIMARY KEY clause is not allowed by default on CREATE TABLE DDL';
CREATE TABLE users_explicit_primary_key_uuid
(
    uid UUID,
    name String,
    age Int16
)
ENGINE = MergeTree
ORDER BY (uid, name)
PRIMARY KEY (uid); -- { serverError ILLEGAL_COLUMN }

SELECT 'Assert UUID primary key is allowed when allow_uuid_key set to true';
DROP TABLE IF EXISTS users_uuid_attach;
CREATE TABLE users_uuid_attach
(
    uid UUID,
    age UInt8
)
ENGINE = MergeTree
ORDER BY uid
SETTINGS allow_uuid_key = 1;

INSERT INTO users_uuid_attach VALUES ('00000000-0000-0000-0000-000000000001', 1);

SELECT 'Assert pre-existing table with UUID primary key and allow_uuid_key reset to default is allowed';
ALTER TABLE users_uuid_attach RESET SETTING allow_uuid_key;
DETACH TABLE users_uuid_attach;
ATTACH TABLE users_uuid_attach;

SELECT count() FROM users_uuid_attach;

DROP TABLE users_uuid_attach;
