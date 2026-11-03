SET allow_experimental_analyzer = 1;
SET enable_parallel_replicas = 0;

CREATE TABLE t0 (c0 Int, c1 Int ALIAS 1) ENGINE = Memory;
CREATE TABLE t0__fuzz_42 (`c0` Array(Nullable(UInt32)), `c1` IPv4 ALIAS 1) ENGINE = Memory;
SELECT c0 FROM remote('localhost', currentDatabase(), 't0') AS tx INNER JOIN t0__fuzz_42 USING (c1); -- { serverError NOT_IMPLEMENTED }

DROP TABLE t0;

CREATE TABLE t0 (c0 Int ALIAS 1, c1 Int) ENGINE = Memory;
SELECT 1 FROM (SELECT 1 AS c0 FROM t0, remote('localhost:9000', currentDatabase(), 't0') ty) tx JOIN t0 ON tx.c0 = t0.c0;

(
    SELECT 1 x, x y FROM remote('localhost', currentDatabase(), t0) tx
)
UNION ALL
(
    SELECT 1, c0 FROM t0
);
