SET session_timezone = 'Etc/UTC';

DROP TABLE IF EXISTS test_iso_variant;

CREATE TABLE test_iso_variant (v Variant(DateTime64(3), String)) ENGINE = Memory;

INSERT INTO test_iso_variant FORMAT JSONEachRow {"v": ISODate("2024-05-29T23:16:12.256")};

INSERT INTO test_iso_variant FORMAT JSONEachRow {"v": new ISODate("2024-05-29T23:16:12.256Z")};

INSERT INTO test_iso_variant FORMAT JSONEachRow {"v": "plain string"};

SELECT v, variantType(v) FROM test_iso_variant ORDER BY variantType(v), toString(v);

DROP TABLE test_iso_variant;
