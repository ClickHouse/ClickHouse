DROP TABLE IF EXISTS test_iso;

CREATE TABLE test_iso ( ts DateTime64(3, 'UTC')) ENGINE = Memory;

INSERT INTO test_iso FORMAT JSONEachRow {"ts": new ISODate("2024-05-29T23:16:12.256Z")};

SELECT ts FROM test_iso;
