-- Validate functions on lazy replicated columns do not throw on values that should have been filtered out.

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

CREATE TABLE t1 (a UInt128, b UInt128) ENGINE = MergeTree ORDER BY a;
CREATE TABLE t2 (a UInt128) ENGINE = MergeTree ORDER BY a;

INSERT INTO t1 VALUES (1, 0), (2, 5), (3, 0), (4, 10);
INSERT INTO t2 VALUES (2), (2), (4);

SET enable_join_runtime_filters = 0;
SET enable_lazy_columns_replication = 1;

SELECT intDiv(t1.a, t1.b) FROM t1 JOIN t2 ON t1.a = t2.a LIMIT 3;
