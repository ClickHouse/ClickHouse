-- Tags: no-ordinary-database, no-replicated-database
-- Refreshable MVs with non-replicated inner tables are refused on a Replicated database, so this
-- test (which parses and creates refreshable MVs) is restricted to Atomic databases.
-- Tests parsing and formatting of REFRESH ... IF CHANGED (issue #108713).

DROP TABLE IF EXISTS mv1 SYNC;
DROP TABLE IF EXISTS mv2 SYNC;
DROP TABLE IF EXISTS mv3 SYNC;
DROP TABLE IF EXISTS mv4 SYNC;
DROP TABLE IF EXISTS src SYNC;

CREATE TABLE src (x UInt32) ENGINE = MergeTree ORDER BY x;

CREATE MATERIALIZED VIEW mv1 REFRESH EVERY 1 HOUR IF CHANGED ENGINE = MergeTree ORDER BY x AS SELECT x FROM src;
SELECT 'every', position(create_table_query, 'REFRESH EVERY 1 HOUR IF CHANGED') > 0 FROM system.tables WHERE database = currentDatabase() AND name = 'mv1';

CREATE MATERIALIZED VIEW mv2 REFRESH AFTER 1 HOUR IF CHANGED ENGINE = MergeTree ORDER BY x AS SELECT x FROM src;
SELECT 'after', position(create_table_query, 'REFRESH AFTER 1 HOUR IF CHANGED') > 0 FROM system.tables WHERE database = currentDatabase() AND name = 'mv2';

CREATE MATERIALIZED VIEW mv3 REFRESH EVERY 2 HOUR IF CHANGED RANDOMIZE FOR 1 HOUR ENGINE = MergeTree ORDER BY x AS SELECT x FROM src;
SELECT 'randomize', position(create_table_query, 'IF CHANGED RANDOMIZE FOR') > 0 FROM system.tables WHERE database = currentDatabase() AND name = 'mv3';

-- Without IF CHANGED it must not appear.
CREATE MATERIALIZED VIEW mv4 REFRESH EVERY 1 HOUR ENGINE = MergeTree ORDER BY x AS SELECT x FROM src;
SELECT 'absent', position(create_table_query, 'IF CHANGED') FROM system.tables WHERE database = currentDatabase() AND name = 'mv4';

-- ALTER can add and remove IF CHANGED.
ALTER TABLE mv4 MODIFY REFRESH EVERY 1 HOUR IF CHANGED;
SELECT 'alter add', position(create_table_query, 'IF CHANGED') > 0 FROM system.tables WHERE database = currentDatabase() AND name = 'mv4';
ALTER TABLE mv4 MODIFY REFRESH EVERY 1 HOUR;
SELECT 'alter remove', position(create_table_query, 'IF CHANGED') FROM system.tables WHERE database = currentDatabase() AND name = 'mv4';

DROP TABLE mv1 SYNC;
DROP TABLE mv2 SYNC;
DROP TABLE mv3 SYNC;
DROP TABLE mv4 SYNC;
DROP TABLE src SYNC;
