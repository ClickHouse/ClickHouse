drop table if exists test_table;
CREATE TABLE test_table
(
    col1 String,
    col2 String,
    INDEX test_table_col2_idx col2 TYPE set(0) GRANULARITY 1
) ENGINE = MergeTree()
      ORDER BY col1
AS SELECT 'v1', 'v2';

SELECT * FROM test_table
WHERE 1 == 1 AND col1 == col1 OR
       0 AND col2 == NULL;

DROP TABLE test_table;

CREATE TABLE test_table
(
    col Nullable(Boolean),
    INDEX col_idx col TYPE set(0)
)
ENGINE = MergeTree()
ORDER BY tuple()
SETTINGS allow_nullable_key = 1;

INSERT INTO test_table VALUES(DEFAULT), (DEFAULT);

SELECT count() FROM test_table where col OR col IS NULL;
DROP TABLE test_table;

CREATE TABLE test_table
(
    col Nullable(Boolean),
    INDEX col_idx col TYPE set(0)
)
ENGINE = MergeTree()
ORDER BY tuple()
SETTINGS allow_nullable_key = 1;

INSERT INTO test_table VALUES(DEFAULT), (DEFAULT), (TRUE);

SELECT count() FROM test_table where col OR col IS NULL;
DROP TABLE test_table;

CREATE TABLE test_table
(
    col Nullable(Boolean),
    INDEX col_idx col TYPE set(0)
)
ENGINE = MergeTree()
ORDER BY tuple()
SETTINGS allow_nullable_key = 1;

INSERT INTO test_table VALUES(DEFAULT), (DEFAULT), (FALSE);

SELECT count() FROM test_table where col OR col IS NULL;
DROP TABLE test_table;
