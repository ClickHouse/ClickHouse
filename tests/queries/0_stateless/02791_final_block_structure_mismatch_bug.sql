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

optimize table test_block_mismatch final;
system stop merges test_block_mismatch;

INSERT INTO test_block_mismatch VALUES (2, toDateTime('2023-01-01 12:12:12'));
INSERT INTO test_block_mismatch VALUES (2, toDateTime('2023-01-01 12:12:12'));
-- one lonely part in 2023-02-02 partition and 3 parts in 2023-01-01 partition.
-- lonely part will not be processed by PartsSplitter and 2023-01-01's parts will be - previously this led to the `Block structure mismatch in Pipe::unitePipes` exception.
SELECT count(*) FROM test_block_mismatch FINAL;


-- variations of the test above with slightly modified table definitions

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

optimize table test_block_mismatch_sk1 final;
system stop merges test_block_mismatch_sk1;

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

optimize table test_block_mismatch_sk2 final;
system stop merges test_block_mismatch_sk2;

INSERT INTO test_block_mismatch_sk2 VALUES (2, toDateTime('2023-01-01 12:12:12'));
INSERT INTO test_block_mismatch_sk2 VALUES (2, toDateTime('2023-01-01 12:12:12'));
SELECT count(*) FROM test_block_mismatch_sk2 FINAL;
