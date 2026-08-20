-- { echo }

SET allow_experimental_nullable_tuple_type = 1;

DROP TABLE IF EXISTS test;
CREATE TABLE test (id UInt32, tup Nullable(Tuple(s Nullable(String))))
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 1, min_bytes_for_wide_part = 0;

INSERT INTO test VALUES
    (0, NULL), (1, ('a')), (2, (NULL)),
    (3, NULL), (4, ('b')), (5, (NULL)),
    (6, NULL), (7, ('c')), (8, (NULL)),
    (9, NULL), (10, ('d')), (11, (NULL)),
    (12, NULL), (13, ('e')), (14, (NULL)),
    (15, NULL), (16, ('f')), (17, (NULL)),
    (18, NULL), (19, ('g')), (20, (NULL)),
    (21, NULL), (22, ('h')), (23, (NULL)),
    (24, NULL), (25, ('i')), (26, (NULL)),
    (27, NULL), (28, ('j')), (29, (NULL));

SELECT countIf(isNull(tup.s)), countIf(NOT isNull(tup.s)) FROM test SETTINGS max_block_size = 1;
SELECT countIf(isNull(tup.s)), countIf(NOT isNull(tup.s)) FROM test SETTINGS max_block_size = 1000;
SELECT sum(cityHash64(id, ifNull(tup.s, ''), isNull(tup.s))) FROM test SETTINGS max_block_size = 1;
SELECT sum(cityHash64(id, ifNull(tup.s, ''), isNull(tup.s))) FROM test SETTINGS max_block_size = 1000;
SELECT id, tup.s FROM test ORDER BY id SETTINGS max_block_size = 1;

DROP TABLE test;
