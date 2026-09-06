SET date_time_input_format = 'basic';
SET session_timezone = 'Asia/Kolkata';

-- A value with no trailing 'Z' must stay in the column time zone, not get forced to UTC.
DROP TABLE IF EXISTS test_iso;

CREATE TABLE test_iso (ts DateTime64(3, 'Asia/Kolkata')) ENGINE = Memory;

INSERT INTO test_iso FORMAT JSONEachRow {"ts": ISODate("2024-05-29T23:16:12.256")};

INSERT INTO test_iso FORMAT JSONEachRow {"ts": new ISODate("2024-05-29T23:16:12.256")};

SELECT ts FROM test_iso ORDER BY ts;

DROP TABLE test_iso;
