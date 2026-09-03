CREATE TABLE t (x Int8) ENGINE MergeTree ORDER BY x;
INSERT INTO t VALUES (1);
SELECT arrayJoin([tuple((toNullable(10) * toLowCardinality(20)) < materialize(30))]) AS row FROM t WHERE row.1 = 0;