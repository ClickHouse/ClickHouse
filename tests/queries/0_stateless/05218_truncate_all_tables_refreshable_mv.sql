-- Tags: no-replicated-database, no-ordinary-database
-- no-replicated-database: `TRUNCATE ALL TABLES` is not supported for `Replicated` databases.
-- no-ordinary-database: refreshable materialized views require an `Atomic` database.

DROP TABLE IF EXISTS truncate_mv;
DROP TABLE IF EXISTS truncate_mv_src;

CREATE TABLE truncate_mv_src (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO truncate_mv_src VALUES (1), (2);

CREATE MATERIALIZED VIEW truncate_mv REFRESH EVERY 1 YEAR (id UInt64)
ENGINE = MergeTree ORDER BY id EMPTY AS SELECT id FROM truncate_mv_src;

SYSTEM REFRESH VIEW truncate_mv;
SYSTEM WAIT VIEW truncate_mv;
SELECT count() FROM truncate_mv;

TRUNCATE ALL TABLES FROM {CLICKHOUSE_DATABASE:Identifier};
SELECT count() FROM truncate_mv_src;
SELECT count() FROM truncate_mv;
SELECT count() FROM system.view_refreshes WHERE database = currentDatabase() AND view = 'truncate_mv';

INSERT INTO truncate_mv_src VALUES (3), (4), (5);
SYSTEM REFRESH VIEW truncate_mv;
SYSTEM WAIT VIEW truncate_mv;
SELECT count(), sum(id) FROM truncate_mv;

DROP TABLE truncate_mv;
DROP TABLE truncate_mv_src;
