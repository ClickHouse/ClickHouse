-- Tests that a materialized view cannot be created on a source table that does not support inserts,
-- because such a view would never be triggered and is almost always a user mistake.
-- https://github.com/ClickHouse/ClickHouse/issues/5207

DROP TABLE IF EXISTS src;
DROP TABLE IF EXISTS underlying;
DROP TABLE IF EXISTS mv;

CREATE TABLE underlying (n1 Int8, n2 Int8) ENGINE = MergeTree ORDER BY n1;

-- Merge: read-only, cannot be a source for a materialized view.
CREATE TABLE src AS underlying ENGINE = Merge(currentDatabase(), 'underlying');
CREATE MATERIALIZED VIEW mv ENGINE = Memory AS SELECT n1, n2 FROM src; -- { serverError BAD_ARGUMENTS }
DROP TABLE src;

-- A normal View: read-only, cannot be a source for a materialized view.
CREATE VIEW src AS SELECT n1, n2 FROM underlying;
CREATE MATERIALIZED VIEW mv ENGINE = Memory AS SELECT n1, n2 FROM src; -- { serverError BAD_ARGUMENTS }
DROP TABLE src;

-- A regular MergeTree source is fine.
CREATE MATERIALIZED VIEW mv ENGINE = Memory AS SELECT n1, n2 FROM underlying;
INSERT INTO underlying VALUES (1, 2);
SELECT n1, n2 FROM mv ORDER BY n1;
DROP TABLE mv;

-- A Buffer table supports inserts, so it is allowed as a source.
CREATE TABLE src AS underlying ENGINE = Buffer(currentDatabase(), underlying, 1, 1, 1, 1, 1, 1, 1);
CREATE MATERIALIZED VIEW mv ENGINE = Memory AS SELECT n1, n2 FROM src;
DROP TABLE mv;
DROP TABLE src;

-- A refreshable materialized view recomputes the query on a schedule and registers no source
-- dependency, so it is allowed even over a read-only `Merge` table.
CREATE TABLE src AS underlying ENGINE = Merge(currentDatabase(), 'underlying');
CREATE MATERIALIZED VIEW mv REFRESH EVERY 1 YEAR ENGINE = Memory AS SELECT n1, n2 FROM src;
DROP TABLE mv;
DROP TABLE src;

DROP TABLE underlying;
