SET allow_experimental_hybrid_table = 1,
    prefer_localhost_replica = 0;

DROP TABLE IF EXISTS test_hybrid_alias_cast;
DROP TABLE IF EXISTS test_hybrid_alias_after;
DROP TABLE IF EXISTS test_hybrid_alias_before;

CREATE TABLE test_hybrid_alias_after
(
    a UInt32,
    arr Array(String),
    arr_1 ALIAS arr[1]
)
ENGINE = MergeTree()
ORDER BY (a, arr[1])
SETTINGS index_granularity = 1;

CREATE TABLE test_hybrid_alias_before
(
    a UInt32,
    arr Array(String),
    arr_1 MATERIALIZED arr[1]
)
ENGINE = MergeTree()
ORDER BY (a, arr_1)
SETTINGS index_granularity = 1;

INSERT INTO test_hybrid_alias_after VALUES (1, ['foo1', 'bar1_after']), (2, ['foo2', 'bar2_after']);
INSERT INTO test_hybrid_alias_before VALUES (1, ['foo1', 'bar1_before']), (2, ['foo2', 'bar2_before']);

CREATE TABLE test_hybrid_alias_cast
(
    a UInt32,
    arr Array(String),
    arr_1 String
)
ENGINE = Hybrid(
    remote('127.0.0.1:9000', currentDatabase(), 'test_hybrid_alias_after'),
    a >= 2,
    remote('127.0.0.1:9000', currentDatabase(), 'test_hybrid_alias_before'),
    a < 2
);

SELECT 'test1';
SELECT * FROM test_hybrid_alias_cast WHERE arr_1 like 'foo%' ORDER BY a;

DROP TABLE test_hybrid_alias_cast;
DROP TABLE test_hybrid_alias_after;
DROP TABLE test_hybrid_alias_before;

DROP TABLE IF EXISTS test_hybrid_alias_predicate;
DROP TABLE IF EXISTS test_hybrid_alias_predicate_left;
DROP TABLE IF EXISTS test_hybrid_alias_predicate_right;

CREATE TABLE test_hybrid_alias_predicate_left
(
    id Int32,
    value Int32,
    date_col Date,
    computed ALIAS value * 2,
    sum_alias ALIAS id + value
)
ENGINE = MergeTree()
ORDER BY (date_col, id)
PARTITION BY toYYYYMM(date_col);

CREATE TABLE test_hybrid_alias_predicate_right
(
    id Int32,
    value Int32,
    date_col Date,
    computed ALIAS value * 2,
    sum_alias ALIAS id + value
)
ENGINE = MergeTree()
ORDER BY (date_col, id)
PARTITION BY toYYYYMM(date_col);

INSERT INTO test_hybrid_alias_predicate_left (id, value, date_col) VALUES
    (1, 10, '2025-01-15'),
    (2, 20, '2025-01-16'),
    (3, 30, '2025-01-17');

INSERT INTO test_hybrid_alias_predicate_right (id, value, date_col) VALUES
    (4, 40, '2025-01-10'),
    (5, 50, '2025-01-11'),
    (6, 60, '2025-01-12');

CREATE TABLE test_hybrid_alias_predicate
(
    id Int32,
    value Int32,
    date_col Date,
    computed Int64,
    sum_alias Int64
)
ENGINE = Hybrid(
    remote('127.0.0.1:9000', currentDatabase(), 'test_hybrid_alias_predicate_left'),
    computed >= 60,
    remote('127.0.0.1:9000', currentDatabase(), 'test_hybrid_alias_predicate_right'),
    computed < 60
);

SELECT 'Hybrid segment predicates with alias columns';
SELECT id, value, computed, sum_alias FROM test_hybrid_alias_predicate;

DROP TABLE test_hybrid_alias_predicate;
DROP TABLE test_hybrid_alias_predicate_left;
DROP TABLE test_hybrid_alias_predicate_right;

DROP TABLE IF EXISTS test_hybrid_ephem;
DROP TABLE IF EXISTS test_hybrid_ephem_after;
DROP TABLE IF EXISTS test_hybrid_ephem_before;

CREATE TABLE test_hybrid_ephem_after
(
    id UInt64,
    unhexed String EPHEMERAL,
    hexed FixedString(4) DEFAULT unhex(unhexed)
)
ENGINE = MergeTree()
ORDER BY id;

CREATE TABLE test_hybrid_ephem_before
(
    id UInt64,
    hexed FixedString(4)
)
ENGINE = MergeTree()
ORDER BY id;

INSERT INTO test_hybrid_ephem_after (id, unhexed) VALUES (1, '5a90b714');
INSERT INTO test_hybrid_ephem_before (id, hexed) VALUES (10, unhex('01020304'));

CREATE TABLE test_hybrid_ephem
(
    id UInt64,
    unhexed String EPHEMERAL,
    hexed FixedString(4) DEFAULT unhex(unhexed)
)
ENGINE = Hybrid(
    remote('127.0.0.1:9000', currentDatabase(), 'test_hybrid_ephem_after'),
    id < 10,
    remote('127.0.0.1:9000', currentDatabase(), 'test_hybrid_ephem_before'),
    id >= 10
);

SELECT 'Insert into Hybrid with EPHEMERAL column';
INSERT INTO test_hybrid_ephem (id, unhexed) VALUES (2, '0a0b0c0d');
SELECT id, hex(hexed) FROM test_hybrid_ephem_after WHERE id = 2;

SELECT 'Select from Hybrid with EPHEMERAL column';
SELECT id, hex(hexed) FROM test_hybrid_ephem ORDER BY id;

DROP TABLE test_hybrid_ephem;
DROP TABLE test_hybrid_ephem_after;
DROP TABLE test_hybrid_ephem_before;
