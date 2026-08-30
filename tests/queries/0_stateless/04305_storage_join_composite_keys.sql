-- Composite (multi-column) keys in the Join table engine.
-- Reproduces https://github.com/ClickHouse/ClickHouse/issues/7018 which used to fail with
-- "Unsupported JOIN keys in StorageJoin. Type: 8".

DROP TABLE IF EXISTS test;
DROP TABLE IF EXISTS max_versions_test;

CREATE TABLE test (id UInt64, ver UInt8) ENGINE = Memory;
INSERT INTO test VALUES (1, 0) (1, 1) (1, 2) (2, 0) (2, 1) (2, 2);

CREATE TABLE max_versions_test ENGINE = Join(ANY, INNER, id, ver)
AS SELECT id, max(ver) AS ver FROM test GROUP BY id;

SELECT 'ANY INNER';
SELECT * FROM test ANY INNER JOIN max_versions_test USING (id, ver) ORDER BY id, ver;

DROP TABLE max_versions_test;

CREATE TABLE max_versions_test ENGINE = Join(ANY, LEFT, id, ver)
AS SELECT id, max(ver) AS ver FROM test GROUP BY id;

SELECT 'ANY LEFT';
SELECT id, ver FROM test ANY LEFT JOIN max_versions_test USING (id, ver) ORDER BY id, ver;

DROP TABLE max_versions_test;

CREATE TABLE max_versions_test ENGINE = Join(ALL, INNER, id, ver)
AS SELECT id, max(ver) AS ver FROM test GROUP BY id;

SELECT 'ALL INNER';
SELECT * FROM test ALL INNER JOIN max_versions_test USING (id, ver) ORDER BY id, ver;

DROP TABLE max_versions_test;
DROP TABLE test;
