
DROP TABLE IF EXISTS 02947_table_1;
DROP TABLE IF EXISTS 02947_table_2;

CREATE TABLE 02947_table_1 (id Int32) Engine=MergeTree() ORDER BY id;
CREATE TABLE 02947_table_2 (id Int32) Engine=MergeTree() ORDER BY id;
INSERT INTO 02947_table_1 VALUES (1),(2);
INSERT INTO 02947_table_2 VALUES (3),(4);

SELECT database, table, name FROM system.parts WHERE database = currentDatabase() AND startsWith(table, '02947_table_');
select * from system.dropped_tables_parts format Null;

DROP TABLE 02947_table_1;
DROP TABLE 02947_table_2;
