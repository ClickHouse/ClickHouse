CREATE TABLE test
(
    foo String,
    bar String,
)
ENGINE = MergeTree()
ORDER BY (foo, bar);

INSERT INTO test VALUES ('foo', 'bar1');

SELECT COLUMNS(bar, foo) APPLY (length) FROM test;

SELECT COLUMNS(bar, foo, xyz) APPLY (length) FROM test; -- { serverError UNKNOWN_IDENTIFIER }
