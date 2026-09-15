SET session_timezone = 'Etc/UTC';

DROP TABLE IF EXISTS test_iso_split;

CREATE TABLE test_iso_split (id UInt8, ts Nullable(DateTime64(3))) ENGINE = Memory;

INSERT INTO test_iso_split SETTINGS max_read_buffer_size = 1, input_format_parallel_parsing = 0 FORMAT JSONEachRow {"id": 1, "ts": new ISODate("2024-05-29T23:16:12.256")}
{"id": 2, "ts": null}
{"id": 3, "ts": ISODate("2024-05-29T23:16:12.256")}
;

SELECT id, ts FROM test_iso_split ORDER BY id;

DROP TABLE test_iso_split;
