SET do_not_merge_across_partitions_select_final=1;

CREATE TABLE test_block_mismatch
(
    a UInt32,
    b DateTime
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(b)
ORDER BY (toDate(b), a);

INSERT INTO test_block_mismatch VALUES (1, toDateTime('2023-01-01 12:12:12'));
INSERT INTO test_block_mismatch VALUES (1, toDateTime('2023-01-01 12:12:12'));
SELECT count(*) FROM test_block_mismatch FINAL;

INSERT INTO test_block_mismatch VALUES (1, toDateTime('2023-02-02 12:12:12'));
INSERT INTO test_block_mismatch VALUES (1, toDateTime('2023-02-02 12:12:12'));
SELECT count(*) FROM test_block_mismatch FINAL;

INSERT INTO test_block_mismatch VALUES (2, toDateTime('2023-01-01 12:12:12'));
INSERT INTO test_block_mismatch VALUES (2, toDateTime('2023-01-01 12:12:12'));
SELECT count(*) FROM test_block_mismatch FINAL;

CREATE TABLE test_block_mismatch_sk1
(
    a UInt32,
    b DateTime
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(b)
PRIMARY KEY (toDate(b))
ORDER BY (toDate(b), a);

INSERT INTO test_block_mismatch_sk1  VALUES (1, toDateTime('2023-01-01 12:12:12'));
INSERT INTO test_block_mismatch_sk1 VALUES (1, toDateTime('2023-01-01 12:12:12'));
SELECT count(*) FROM test_block_mismatch_sk1 FINAL;

INSERT INTO test_block_mismatch_sk1 VALUES (1, toDateTime('2023-02-02 12:12:12'));
INSERT INTO test_block_mismatch_sk1 VALUES (1, toDateTime('2023-02-02 12:12:12'));
SELECT count(*) FROM test_block_mismatch_sk1 FINAL;

INSERT INTO test_block_mismatch_sk1 VALUES (2, toDateTime('2023-01-01 12:12:12'));
INSERT INTO test_block_mismatch_sk1 VALUES (2, toDateTime('2023-01-01 12:12:12'));
SELECT count(*) FROM test_block_mismatch_sk1 FINAL;

CREATE TABLE test_block_mismatch_sk2
(
    a UInt32,
    b DateTime
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(b)
PRIMARY KEY (a)
ORDER BY (a, toDate(b));

INSERT INTO test_block_mismatch_sk2  VALUES (1, toDateTime('2023-01-01 12:12:12'));
INSERT INTO test_block_mismatch_sk2 VALUES (1, toDateTime('2023-01-01 12:12:12'));
SELECT count(*) FROM test_block_mismatch_sk2 FINAL;

INSERT INTO test_block_mismatch_sk2 VALUES (1, toDateTime('2023-02-02 12:12:12'));
INSERT INTO test_block_mismatch_sk2 VALUES (1, toDateTime('2023-02-02 12:12:12'));
SELECT count(*) FROM test_block_mismatch_sk2 FINAL;

INSERT INTO test_block_mismatch_sk2 VALUES (2, toDateTime('2023-01-01 12:12:12'));
INSERT INTO test_block_mismatch_sk2 VALUES (2, toDateTime('2023-01-01 12:12:12'));
SELECT count(*) FROM test_block_mismatch_sk2 FINAL;
