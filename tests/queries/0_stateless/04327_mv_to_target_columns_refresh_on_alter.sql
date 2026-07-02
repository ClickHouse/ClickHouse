-- Tags: no-parallel
-- ^ uses ON CLUSTER-free DDL but relies on stable DESCRIBE output across an upstream ALTER

DROP TABLE IF EXISTS mv;
DROP TABLE IF EXISTS source;
DROP TABLE IF EXISTS target;

-- An inferred-column TO-target MV must refresh its reported schema when an upstream ALTER
-- changes the column types it reads, so DESCRIBE / SHOW CREATE and reads stay consistent.
CREATE TABLE source (id UInt64, status Enum8('A' = 0, 'B' = 1)) ENGINE = MergeTree ORDER BY id;
CREATE TABLE target (id UInt64, status Enum8('A' = 0, 'B' = 1), cnt UInt64) ENGINE = SummingMergeTree ORDER BY (id, status);
CREATE MATERIALIZED VIEW mv TO target AS SELECT id, status, count() AS cnt FROM source GROUP BY id, status;

SELECT '-- before alter';
SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 'mv' ORDER BY position;

ALTER TABLE source MODIFY COLUMN status Enum8('A' = 0, 'B' = 1, 'C' = 2);
ALTER TABLE target MODIFY COLUMN status Enum8('A' = 0, 'B' = 1, 'C' = 2);

SELECT '-- after alter (mv must report the new enum)';
SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 'mv' ORDER BY position;

SELECT '-- reading the mv must not fail on the new enum value';
INSERT INTO source SETTINGS async_insert = 0 VALUES (1, 'C');
SELECT * FROM mv WHERE id = 1 ORDER BY id;

DROP TABLE mv;
DROP TABLE source;
DROP TABLE target;

-- An explicit-column MV keeps its declared types: an upstream ALTER must not rewrite them.
SET allow_materialized_view_with_bad_select = 1;
DROP TABLE IF EXISTS mv_explicit;
DROP TABLE IF EXISTS src_explicit;
DROP TABLE IF EXISTS dst_explicit;

CREATE TABLE src_explicit (n UInt64) ENGINE = Memory;
CREATE TABLE dst_explicit (n UInt8) ENGINE = Memory;
CREATE MATERIALIZED VIEW mv_explicit TO dst_explicit (n String) AS SELECT n FROM src_explicit;

SELECT '-- explicit mv before alter (declared String)';
SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 'mv_explicit' ORDER BY position;

ALTER TABLE src_explicit MODIFY COLUMN n Int64;

SELECT '-- explicit mv after alter (must stay String, not refreshed)';
SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 'mv_explicit' ORDER BY position;

DROP TABLE mv_explicit;
DROP TABLE src_explicit;
DROP TABLE dst_explicit;

-- A MODIFY QUERY whose output intentionally differs from the target type must be preserved:
-- the MV reports its SELECT-output schema, and an upstream ALTER must not "correct" it.
DROP TABLE IF EXISTS pipe;
DROP TABLE IF EXISTS src;
DROP TABLE IF EXISTS dest;

CREATE TABLE src (v UInt64) ENGINE = Null;
CREATE TABLE dest (v UInt64) ENGINE = MergeTree ORDER BY v;
CREATE MATERIALIZED VIEW pipe TO dest AS SELECT v FROM src;
ALTER TABLE dest ADD COLUMN v2 UInt64;
ALTER TABLE pipe MODIFY QUERY SELECT v * 2 AS v, 1 AS v2 FROM src;

SELECT '-- modify-query mv keeps SELECT-output types (v2 UInt8, not target UInt64)';
SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 'pipe' ORDER BY position;

DROP TABLE pipe;
DROP TABLE src;
DROP TABLE dest;
