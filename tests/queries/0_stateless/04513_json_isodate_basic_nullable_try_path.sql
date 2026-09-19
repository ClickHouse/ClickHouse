SET date_time_input_format = 'basic';
SET session_timezone = 'Asia/Kolkata';

-- Nullable(DateTime64) uses the try (bool) parse path; check it also forces UTC for 'Z'.
-- Uses a non-UTC time zone, unlike 04499's 'Etc/UTC', so the conversion is actually visible.
DROP TABLE IF EXISTS test_iso_nullable;

CREATE TABLE test_iso_nullable (ts Nullable(DateTime64(3, 'Asia/Kolkata'))) ENGINE = Memory;

INSERT INTO test_iso_nullable FORMAT JSONEachRow {"ts": ISODate("2024-05-29T23:16:12.256Z")};

INSERT INTO test_iso_nullable FORMAT JSONEachRow {"ts": new ISODate("2024-05-29T23:16:12.256Z")};

INSERT INTO test_iso_nullable FORMAT JSONEachRow {"ts": null};

SELECT ts FROM test_iso_nullable ORDER BY ts NULLS LAST;

DROP TABLE test_iso_nullable;
