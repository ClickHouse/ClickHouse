DROP TABLE IF EXISTS test.rollup_having;
CREATE TABLE test.rollup_having (
  a Nullable(String),
  b Nullable(String)
) ENGINE = Memory;

INSERT INTO test.rollup_having VALUES (NULL, NULL);
INSERT INTO test.rollup_having VALUES ('a', NULL);
INSERT INTO test.rollup_having VALUES ('a', 'b');

SELECT a, b, count(*) FROM test.rollup_having GROUP BY a, b WITH ROLLUP HAVING a IS NOT NULL;
SELECT a, b, count(*) FROM test.rollup_having GROUP BY a, b WITH ROLLUP HAVING a IS NOT NULL and b IS NOT NULL;

SELECT a, b, count(*) FROM test.rollup_having GROUP BY a, b WITH ROLLUP WITH TOTALS HAVING a IS NOT NULL;
SELECT a, b, count(*) FROM test.rollup_having GROUP BY a, b WITH ROLLUP WITH TOTALS HAVING a IS NOT NULL and b IS NOT NULL;
