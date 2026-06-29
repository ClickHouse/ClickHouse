-- Tags: no-ordinary-database

SET database_atomic_wait_for_drop_and_detach_synchronously = 0;
DROP TABLE IF EXISTS 25400_dropped_tables;

CREATE TABLE 25400_dropped_tables (id Int32) Engine=MergeTree() ORDER BY id;
INSERT INTO 25400_dropped_tables VALUES (1),(2);
INSERT INTO 25400_dropped_tables VALUES (3),(4);
DROP TABLE 25400_dropped_tables;

SELECT table, engine FROM system.dropped_tables WHERE database = currentDatabase() LIMIT 1;
SELECT database, table, name FROM system.dropped_tables_parts WHERE database = currentDatabase() and table = '25400_dropped_tables';
