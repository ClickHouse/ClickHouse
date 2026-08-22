DROP TABLE IF EXISTS test.stripelog_alter_add_column;

CREATE TABLE test.stripelog_alter_add_column
(
    a UInt64,
    s String
)
ENGINE = StripeLog;

INSERT INTO test.stripelog_alter_add_column VALUES (1, 'one'), (2, 'two');

ALTER TABLE test.stripelog_alter_add_column ADD COLUMN b UInt64 DEFAULT a + 10;

SELECT b FROM test.stripelog_alter_add_column ORDER BY b;
SELECT * FROM test.stripelog_alter_add_column ORDER BY a;

INSERT INTO test.stripelog_alter_add_column (a, s, b) VALUES (3, 'three', 999);

SELECT a, b
FROM test.stripelog_alter_add_column
ORDER BY a
SETTINGS max_threads = 1, max_streams_to_max_threads_ratio = 1;

ALTER TABLE test.stripelog_alter_add_column ADD COLUMN c UInt8;

SELECT c FROM test.stripelog_alter_add_column ORDER BY c;
SELECT * FROM test.stripelog_alter_add_column ORDER BY a;

DROP TABLE test.stripelog_alter_add_column;
