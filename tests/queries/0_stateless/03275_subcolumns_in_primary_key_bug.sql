DROP TABLE IF EXISTS test1;

CREATE TABLE test1
(
    `t` Tuple(a Boolean)
)
ENGINE = MergeTree
ORDER BY t.a;

INSERT INTO test1 FORMAT Values (tuple(true)), (tuple(false));

ALTER TABLE test1 (DELETE WHERE t.a) SETTINGS alter_sync = 2, mutations_sync = 2;

SELECT * FROM test1;
