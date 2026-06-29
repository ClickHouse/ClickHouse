-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/105021
--
-- After `EXCHANGE TABLES a, b` where a materialized view `mv` reads from `a`,
-- inserts into `a` must continue to feed the `MV` target table. `INSERT INTO b`
-- must not fire the `MV` (b is now an unrelated table at that name).
--
-- Was broken by https://github.com/ClickHouse/ClickHouse/pull/98779 — both
-- inserts silently dropped because the source-view edge had been cross-swapped
-- to follow the data instead of the name. Restored by keying source-view edges
-- by name on exchange.

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
