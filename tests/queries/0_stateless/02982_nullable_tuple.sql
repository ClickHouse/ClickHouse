CREATE TABLE test
(
    x Nullable(Tuple(UInt64, UInt64))
)
ENGINE = MergeTree()
ORDER BY x;

INSERT INTO test VALUES (NULL), (0, 0);

SELECT x FROM test ORDER BY x;
