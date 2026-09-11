-- INSERT SELECT from a parameterized view resolved the view through the legacy interpreter when
-- parallel_distributed_insert_select was enabled, so views only the analyzer can resolve failed.
SET enable_analyzer = 1;
SET parallel_distributed_insert_select = 2;

CREATE TABLE t (id UInt8, p String) ENGINE = MergeTree ORDER BY id;
-- `SELECT i.*` over two joins yields columns named `i.p` in the legacy interpreter.
CREATE VIEW v AS SELECT count() AS c FROM (SELECT i.* FROM t AS i LEFT JOIN t AS a ON i.p = a.p LEFT JOIN t AS b ON i.p = b.p WHERE i.id = {id:UInt8}) AS i WHERE i.p = 'a';
CREATE TABLE d (c UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/d', '1') ORDER BY c;

INSERT INTO d SELECT * FROM v(id = 1);
SELECT * FROM d;

DROP TABLE d SYNC;
