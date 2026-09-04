-- Tags: no-fasttest, no-replicated-database
-- Tag no-fasttest: Depends on AWS
-- Tag no-replicated-database: Named collection is used

-- `upload_checksum_algorithm` is accepted as an `S3` named collection key, and an upload through
-- that collection attaches the requested flexible checksum.

DROP NAMED COLLECTION IF EXISTS collection_05076;
CREATE NAMED COLLECTION collection_05076 AS
    url = 'http://localhost:11111/test/05076_collection.csv',
    access_key_id = 'test',
    secret_access_key = 'testtest',
    format = 'CSV',
    structure = 'number UInt64',
    upload_checksum_algorithm = 'SHA256';

INSERT INTO TABLE FUNCTION s3(collection_05076) SELECT number FROM numbers(10) SETTINGS s3_truncate_on_insert = 1;

SELECT count(), sum(number) FROM s3(collection_05076);

DROP NAMED COLLECTION collection_05076;
