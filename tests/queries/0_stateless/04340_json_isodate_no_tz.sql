DROP TABLE IF EXISTS test_iso;

CREATE TABLE test_iso ( ts DateTime64(3)) ENGINE = Memory;

INSERT INTO test_iso FORMAT JSONEachRow {"ts": ISODate("2024-05-29T23:16:12.256")};

SELECT toString(ts) FROM test_iso;
