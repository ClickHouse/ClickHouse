DROP TABLE IF EXISTS test_prewhere_default_column;
DROP TABLE IF EXISTS test_prewhere_column_type;

CREATE TABLE test_prewhere_default_column (APIKey UInt8, SessionType UInt8) ENGINE = MergeTree() PARTITION BY APIKey ORDER BY tuple();
INSERT INTO test_prewhere_default_column VALUES( 42, 42 );
ALTER TABLE test_prewhere_default_column ADD COLUMN OperatingSystem UInt64 DEFAULT SessionType+1;

SELECT OperatingSystem FROM test_prewhere_default_column PREWHERE SessionType = 42;


CREATE TABLE test_prewhere_column_type (`a` LowCardinality(String), `x` Nullable(Int32)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO test_prewhere_column_type VALUES ('', 2);

SELECT a, y FROM test_prewhere_column_type prewhere (x = 2) AS y;
SELECT a, toTypeName(x = 2), toTypeName(x) FROM test_prewhere_column_type where (x = 2) AS y;

DROP TABLE test_prewhere_default_column;
DROP TABLE test_prewhere_column_type;
