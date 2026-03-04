CREATE TABLE t0 (c0 Array(Int), c1 Tuple()) ENGINE = MergeTree() ORDER BY tuple();

INSERT INTO t0 (c0, c1) VALUES ([1], ()), ([], ());

SELECT * FROM t0 ARRAY JOIN c0 ORDER BY c1;
