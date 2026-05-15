-- Reproducer for https://github.com/ClickHouse/ClickHouse/issues/105021
--
-- After `EXCHANGE TABLES a, b` where a materialized view `mv` reads from `a`,
-- inserts into both `a` and `b` silently stop feeding the MV target table.
-- Regression introduced by https://github.com/ClickHouse/ClickHouse/pull/98779
-- (correct on v26.4.2.10, broken on master).
--
-- This test pins the current buggy behavior so the regression is locked down.
-- When the bug is fixed, the reference must be updated: at least one of the
-- post-exchange `count() FROM dst` assertions should change.

DROP TABLE IF EXISTS mv;
DROP TABLE IF EXISTS dst;
DROP TABLE IF EXISTS a;
DROP TABLE IF EXISTS b;

CREATE TABLE a (id Int32, key String) ENGINE = Null;
CREATE TABLE b (id Int32, key String) ENGINE = Null;
CREATE TABLE dst (id Int32, key String) ENGINE = MergeTree ORDER BY id;
CREATE MATERIALIZED VIEW mv TO dst AS SELECT id, key FROM a;

INSERT INTO a VALUES (1, 'a-before');
SELECT 'count after first insert', count() FROM dst;

SELECT 'before exchange, deps of a', dependencies_table
FROM system.tables WHERE database = currentDatabase() AND name = 'a';
SELECT 'before exchange, deps of b', dependencies_table
FROM system.tables WHERE database = currentDatabase() AND name = 'b';

EXCHANGE TABLES a AND b;

SELECT 'after exchange, deps of a', dependencies_table
FROM system.tables WHERE database = currentDatabase() AND name = 'a';
SELECT 'after exchange, deps of b', dependencies_table
FROM system.tables WHERE database = currentDatabase() AND name = 'b';

INSERT INTO a VALUES (2, 'a-after');
SELECT 'after insert into a, count in dst', count() FROM dst;

INSERT INTO b VALUES (3, 'b-after');
SELECT 'after insert into b, count in dst', count() FROM dst;

DROP TABLE mv;
DROP TABLE dst;
DROP TABLE a;
DROP TABLE b;
