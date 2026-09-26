SET date_time_input_format = 'basic';
SET session_timezone = 'Asia/Kolkata';

-- Variant disambiguation uses the try (bool) parse path; check it also forces UTC for 'Z'.
-- Uses a non-UTC time zone, unlike 04500's 'Etc/UTC', so the conversion is actually visible.
DROP TABLE IF EXISTS test_iso_variant;

CREATE TABLE test_iso_variant (v Variant(DateTime64(3, 'Asia/Kolkata'), String)) ENGINE = Memory;

INSERT INTO test_iso_variant FORMAT JSONEachRow {"v": ISODate("2024-05-29T23:16:12.256Z")};

INSERT INTO test_iso_variant FORMAT JSONEachRow {"v": new ISODate("2024-05-29T23:16:12.256Z")};

INSERT INTO test_iso_variant FORMAT JSONEachRow {"v": "plain string"};

SELECT v, variantType(v) FROM test_iso_variant ORDER BY variantType(v), toString(v);

DROP TABLE test_iso_variant;
