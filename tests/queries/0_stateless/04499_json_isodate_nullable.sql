SET session_timezone = 'Etc/UTC';

DROP TABLE IF EXISTS test_iso_nullable;

CREATE TABLE test_iso_nullable (ts Nullable(DateTime64(3))) ENGINE = Memory;

INSERT INTO test_iso_nullable FORMAT JSONEachRow {"ts": ISODate("2024-05-29T23:16:12.256")};

INSERT INTO test_iso_nullable FORMAT JSONEachRow {"ts": new ISODate("2024-05-29T23:16:12.256Z")};

INSERT INTO test_iso_nullable FORMAT JSONEachRow {"ts": null};

SELECT ts FROM test_iso_nullable ORDER BY ts NULLS LAST;

DROP TABLE test_iso_nullable;
