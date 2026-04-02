-- https://github.com/ClickHouse/ClickHouse/issues/88312
-- NULL into non-nullable Date32 must resolve identically via VALUES and SELECT
DROP TABLE IF EXISTS test_date32_null;
CREATE TABLE test_date32_null (id Int32, dt Date32) ENGINE = MergeTree() ORDER BY id;

INSERT INTO test_date32_null (id, dt) VALUES (1, NULL);
INSERT INTO test_date32_null (id, dt) SELECT 2, NULL;

SELECT dt FROM test_date32_null ORDER BY id;

DROP TABLE test_date32_null;
