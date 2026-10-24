-- A persistent `Set` / `Join` promotes the staged backup file in `onFinish`, and the sink removes a
-- staged file when it is destroyed before the insert finished. A completed pipeline is also
-- cancelled once it has run to the end, so the sink must not treat that as an unfinished insert and
-- delete the backup it has just published - otherwise the data is gone after a reload.
--
-- `send_table_structure_on_insert_with_inline_data = 0` makes the server parse the inline `VALUES`
-- itself, which is exactly the path that runs the insert as a completed pipeline.

DROP TABLE IF EXISTS persistent_set;
DROP TABLE IF EXISTS persistent_join;

CREATE TABLE persistent_set (k UInt64) ENGINE = Set SETTINGS persistent = 1;
INSERT INTO persistent_set SETTINGS send_table_structure_on_insert_with_inline_data = 0 VALUES (1), (2);

SELECT 'set before detach', count() FROM numbers(4) WHERE number IN persistent_set;
DETACH TABLE persistent_set;
ATTACH TABLE persistent_set;
SELECT 'set after attach', count() FROM numbers(4) WHERE number IN persistent_set;

CREATE TABLE persistent_join (k UInt64, v UInt64) ENGINE = Join(ANY, LEFT, k) SETTINGS persistent = 1;
INSERT INTO persistent_join SETTINGS send_table_structure_on_insert_with_inline_data = 0 VALUES (1, 10), (2, 20);

SELECT 'join before detach', count() FROM persistent_join;
DETACH TABLE persistent_join;
ATTACH TABLE persistent_join;
SELECT 'join after attach', count() FROM persistent_join;

DROP TABLE persistent_set;
DROP TABLE persistent_join;
