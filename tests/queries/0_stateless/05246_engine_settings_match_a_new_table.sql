-- `system.engine_settings` reports what a table created now would get. A creator resolves what a definition
-- leaves out from the global context - `StorageFactory::Arguments::getContext` is the global context - so these
-- rows describe the server and a `SET` in this session must not move them.

-- `Join` reads six of its settings that way.
CREATE TEMPORARY TABLE readings (engine String, name String, value String);

SET join_use_nulls = 0;
INSERT INTO readings SELECT engine, name, value FROM system.engine_settings
WHERE engine = 'Join' AND name = 'join_use_nulls';
SET join_use_nulls = 1;
INSERT INTO readings SELECT engine, name, value FROM system.engine_settings
WHERE engine = 'Join' AND name = 'join_use_nulls';

-- A `Distributed` table takes `background_insert_batch` from the server's `distributed_background_insert_batch`
-- where its own `SETTINGS` clause states nothing, so the engine's row has to be filled the same way.
SET distributed_background_insert_batch = 0;
INSERT INTO readings SELECT engine, name, value FROM system.engine_settings
WHERE engine = 'Distributed' AND name = 'background_insert_batch';
SET distributed_background_insert_batch = 1;
INSERT INTO readings SELECT engine, name, value FROM system.engine_settings
WHERE engine = 'Distributed' AND name = 'background_insert_batch';

SELECT engine, name, uniqExact(value) AS readings_agree, count() FROM readings GROUP BY engine, name ORDER BY engine;

-- The four settings filled that way are reported at all, and their source follows the value, as it does for a
-- table's own rows: two of them take a core setting's value that is not this engine's compiled-in default, so
-- they read `other`, and the two whose core value happens to equal it read `default`. Saying `default` for a
-- value that differs from the `default` column would contradict both that column and the table's own row.
SELECT name, value, `default`, changed, source FROM system.engine_settings
WHERE engine = 'Distributed' AND name LIKE 'background_insert_%' ORDER BY name;
