-- Tags: no-ordinary-database, no-replicated-database
-- Refreshable MVs with non-replicated inner tables are refused on a Replicated database
-- (the refresh runs on one replica but the table is replaced on others), so we restrict
-- this case to Atomic databases.

DROP TABLE IF EXISTS test_mv SYNC;
DROP TABLE IF EXISTS src SYNC;

CREATE TABLE src (x UInt32) ENGINE = MergeTree ORDER BY x;

-- CREATE OR REPLACE MATERIALIZED VIEW with REFRESH parses and executes correctly
CREATE OR REPLACE MATERIALIZED VIEW test_mv
    REFRESH EVERY 1 HOUR
    ENGINE = MergeTree ORDER BY x
    AS SELECT x FROM src;
SELECT name FROM system.tables WHERE database = currentDatabase() AND name = 'test_mv';

-- Replacing the view with a new REFRESH interval also works
CREATE OR REPLACE MATERIALIZED VIEW test_mv
    REFRESH EVERY 2 HOUR
    ENGINE = MergeTree ORDER BY x
    AS SELECT x FROM src;
SELECT name FROM system.tables WHERE database = currentDatabase() AND name = 'test_mv';

DROP TABLE IF EXISTS test_mv SYNC;
DROP TABLE IF EXISTS src SYNC;
