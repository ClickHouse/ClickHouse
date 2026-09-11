-- INSERT SELECT from a parameterized view resolved the view through the legacy interpreter when
-- parallel_distributed_insert_select was enabled, so views only the analyzer can resolve failed.
SET enable_analyzer = 1;
SET parallel_distributed_insert_select = 2;

CREATE TABLE t1 (id UInt8, path String) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t2 (path String) ENGINE = MergeTree ORDER BY path;
CREATE TABLE t3 (path String) ENGINE = MergeTree ORDER BY path;
INSERT INTO t1 VALUES (1, 'a'), (1, 'b'), (2, 'c');

-- `SELECT i.*` over two joins yields columns named `i.path` in the legacy interpreter.
CREATE VIEW v_inner AS SELECT i.* FROM t1 AS i LEFT JOIN t2 ON i.path = t2.path LEFT JOIN t3 ON i.path = t3.path WHERE i.id = {id:UInt8};
CREATE VIEW v_outer AS WITH issues AS (SELECT * FROM v_inner(id = {id:UInt8})) SELECT count() AS c, countIf(i.path = 'a') AS a FROM issues AS i;
CREATE TABLE t_dst (c UInt64, a UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_dst', '1') ORDER BY c;

INSERT INTO t_dst SELECT * FROM v_outer(id = 1);
SELECT * FROM t_dst;

DROP TABLE t_dst SYNC;
