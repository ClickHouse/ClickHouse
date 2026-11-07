-- https://github.com/ClickHouse/ClickHouse/issues/97477
SET enable_analyzer = 1;
CREATE TABLE t0 (c0 Int, c1 String, INDEX i0 c1 TYPE text(tokenizer = 'splitByNonAlpha')) ENGINE = MergeTree ORDER BY c0;
INSERT INTO t0 VALUES (1, 'a');
SELECT c1 FROM t0 PREWHERE hasToken(c1, 'a') WHERE hasToken(c1, 'a') QUALIFY c0;
