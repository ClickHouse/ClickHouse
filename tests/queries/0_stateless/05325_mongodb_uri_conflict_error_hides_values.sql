-- Tags: no-fasttest
-- no-fasttest: the MongoDB table engine and dictionary source need USE_MONGODB

-- The MongoDB driver rejects a connection string that gives an option under both its canonical and
-- its deprecated name with different values. Its error must name the options without their values:
-- here they are TLS key passwords. Every value contains the marker ERRSECRET. No MongoDB server is contacted.

-- A fuzzed variant of the CREATE DICTIONARY below could fail with an error of its own.
SET ast_fuzzer_any_query = 0;

CREATE TABLE t_conflict (x String) ENGINE = MongoDB('mongodb://127.0.0.1:27017/db?tlsCertificateKeyFilePassword=ERRSECRET1&sslClientCertificateKeyPassword=ERRSECRET2', 'c'); -- { serverError STD_EXCEPTION }

-- The source of a dictionary is parsed when the dictionary is loaded, so the error reaches whoever calls dictGet.
CREATE DICTIONARY d_conflict (_id String, v String) PRIMARY KEY _id
SOURCE(MONGODB(URI 'mongodb://127.0.0.1:27017/db?tlsCertificateKeyFilePassword=ERRSECRET3&sslClientCertificateKeyPassword=ERRSECRET4' COLLECTION 'c'))
LAYOUT(COMPLEX_KEY_DIRECT());
SELECT dictGet('d_conflict', 'v', tuple('k')); -- { serverError DICTIONARIES_WAS_NOT_LOADED }

SYSTEM FLUSH LOGS query_log;
-- Only the two failed statements of this session: a database can be shared by several runs of the test.
SELECT
    countIf(exception LIKE '%Deprecated option ''sslclientcertificatekeypassword'' conflicts with canonical name ''tlscertificatekeyfilepassword''%'),
    countIf(exception LIKE '%' || 'ERR' || 'SECRET' || '%')
FROM system.query_log
WHERE current_database = currentDatabase() AND event_date >= yesterday()
    AND type IN ('ExceptionBeforeStart', 'ExceptionWhileProcessing')
    AND query_id IN (SELECT query_id FROM system.session_query_ids);

SELECT
    last_exception LIKE '%Deprecated option ''sslclientcertificatekeypassword'' conflicts with canonical name ''tlscertificatekeyfilepassword''%',
    last_exception LIKE '%' || 'ERR' || 'SECRET' || '%'
FROM system.dictionaries WHERE database = currentDatabase() AND name = 'd_conflict';

DROP DICTIONARY d_conflict;
