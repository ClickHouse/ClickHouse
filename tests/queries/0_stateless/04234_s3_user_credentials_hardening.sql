DROP TABLE IF EXISTS s3_credentials_hardening_04234;
DROP NAMED COLLECTION IF EXISTS s3_env_credentials_04234;
DROP NAMED COLLECTION IF EXISTS s3_role_arn_04234;

SELECT *
FROM s3(
    'http://localhost:11111/test/04234.tsv',
    format = 'TSV',
    structure = 'x UInt8',
    use_environment_credentials = 1); -- { serverError BAD_ARGUMENTS }

SELECT *
FROM s3(
    'http://localhost:11111/test/04234.tsv',
    format = 'TSV',
    structure = 'x UInt8',
    extra_credentials(role_arn = 'arn:aws:iam::123456789012:role/test')); -- { serverError ACCESS_DENIED }

CREATE NAMED COLLECTION s3_env_credentials_04234 AS
    url = 'http://localhost:11111/test/04234.tsv',
    use_environment_credentials = 1;

SELECT *
FROM s3(s3_env_credentials_04234, format = 'TSV', structure = 'x UInt8'); -- { serverError ACCESS_DENIED }

DROP NAMED COLLECTION s3_env_credentials_04234;

CREATE NAMED COLLECTION s3_role_arn_04234 AS
    url = 'http://localhost:11111/test/04234.tsv',
    role_arn = 'arn:aws:iam::123456789012:role/test';

SELECT *
FROM s3(s3_role_arn_04234, format = 'TSV', structure = 'x UInt8'); -- { serverError ACCESS_DENIED }

DROP NAMED COLLECTION s3_role_arn_04234;

CREATE TABLE s3_credentials_hardening_04234 (x UInt8)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS disk = disk(
    type = object_storage,
    object_storage_type = s3,
    endpoint = 'http://localhost:11111/test/04234/',
    use_environment_credentials = 1); -- { serverError ACCESS_DENIED }

CREATE TABLE s3_credentials_hardening_04234 (x UInt8)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS disk = disk(
    type = object_storage,
    object_storage_type = s3,
    endpoint = 'http://localhost:11111/test/04234/',
    role_arn = 'arn:aws:iam::123456789012:role/test'); -- { serverError ACCESS_DENIED }

DROP TABLE IF EXISTS s3_credentials_hardening_04234;
