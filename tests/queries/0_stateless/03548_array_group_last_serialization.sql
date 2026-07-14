DROP VIEW IF EXISTS mv0;
DROP TABLE IF EXISTS t0;
DROP TABLE IF EXISTS t1;

CREATE TABLE t0
(
  k0 String,
  v0 Nullable(String)
)
ENGINE = MergeTree()
ORDER BY (k0);

CREATE TABLE t1
(
  k0 String,
  v00 AggregateFunction(groupArrayLast(1), Nullable(String))
)
ENGINE = MergeTree
ORDER BY (k0);

CREATE MATERIALIZED VIEW mv0 TO t1
(
  k0 String,
  v0 AggregateFunction(groupArrayLast(1), Nullable(String))
)
AS SELECT
  k0,
  groupArrayLastState(1)(v0) AS v00
FROM t0
GROUP BY k0;

INSERT INTO t0 SELECT * FROM generateRandom(2, 9) LIMIT 9;

SELECT v00 FROM t1 ORDER BY k0 FORMAT JSONEachRow;

DROP VIEW mv0;
DROP TABLE t0;
DROP TABLE t1;

DROP TABLE IF EXISTS t_memory;
CREATE TABLE t_memory (k Int, v AggregateFunction(groupArrayLast(1), Nullable(String))) ENGINE = Memory;
INSERT INTO t_memory SELECT 1, groupArrayLastState(1)(*) FROM values('v Nullable(String)', null);
INSERT INTO t_memory SELECT 2, groupArrayLastState(1)(*) FROM values('v Nullable(String)', 'foo');
SELECT v FROM t_memory ORDER BY k FORMAT JSONEachRow;
DROP TABLE t_memory;
