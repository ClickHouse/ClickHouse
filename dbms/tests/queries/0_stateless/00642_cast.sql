SELECT CAST(1 AS Enum8('hello' = 1, 'world' = 2));
SELECT cast(1 AS Enum8('hello' = 1, 'world' = 2));

SELECT CAST(1, 'Enum8(\'hello\' = 1, \'world\' = 2)');
SELECT cast(1, 'Enum8(\'hello\' = 1, \'world\' = 2)');

SELECT CAST(1 AS Enum8(
    'hello' = 1, 
    'world' = 2));

SELECT cast(1 AS Enum8(
    'hello' = 1,
    'world' = 2));

SELECT CAST(1, 'Enum8(\'hello\' = 1,\n\t\'world\' = 2)');
SELECT cast(1, 'Enum8(\'hello\' = 1,\n\t\'world\' = 2)');

DROP TABLE IF EXISTS test.cast;
CREATE TABLE test.cast
(
    x UInt8,
    e Enum8
    (
        'hello' = 1,
        'world' = 2
    )
    DEFAULT
    CAST
    (
        x
        AS
        Enum8
        (
            'hello' = 1,
            'world' = 2
        )
    )
) ENGINE = MergeTree ORDER BY e;

SHOW CREATE TABLE test.cast FORMAT TSVRaw;
DESC TABLE test.cast;

INSERT INTO test.cast (x) VALUES (1);
SELECT * FROM test.cast;

DROP TABLE test.cast;
