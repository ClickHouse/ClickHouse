DROP TABLE IF EXISTS t_sparse_intersect;

CREATE TABLE t_sparse_intersect (a UInt64, c Int64) ENGINE = MergeTree
ORDER BY tuple() SETTINGS ratio_of_defaults_for_sparse_serialization = 0.8;

SYSTEM STOP MERGES t_sparse_intersect;

INSERT INTO t_sparse_intersect SELECT if (number % 10 = 0, number, 0), number FROM numbers(1000);
INSERT INTO t_sparse_intersect SELECT number, number FROM numbers(1000);

SELECT count() FROM (SELECT * FROM t_sparse_intersect EXCEPT SELECT * FROM t_sparse_intersect);
SELECT count() FROM (SELECT * FROM t_sparse_intersect INTERSECT SELECT * FROM t_sparse_intersect);

DROP TABLE t_sparse_intersect;
