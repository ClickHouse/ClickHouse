DROP TABLE IF EXISTS test_local;

CREATE TABLE test_local
(
    `a` UInt64,
    `e` String ALIAS 'e',
    `f` String ALIAS 'f'
)
ENGINE = Memory;

INSERT INTO test_local VALUES (1);

SELECT e, f
FROM remote('127.0.0.{1,2}', currentDatabase(), test_local)
ORDER BY a;

DROP TABLE IF EXISTS test_local;
