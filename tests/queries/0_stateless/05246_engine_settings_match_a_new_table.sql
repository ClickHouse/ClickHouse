-- `system.engine_settings` reports the value a table created now would get, so for an engine that reads
-- the creating context's settings the rows have to follow that context.

-- `Join` reads six of its settings from the context that creates the table.
SET join_use_nulls = 0;
SELECT name, value FROM system.engine_settings WHERE engine = 'Join' AND name = 'join_use_nulls';
SET join_use_nulls = 1;
SELECT name, value FROM system.engine_settings WHERE engine = 'Join' AND name = 'join_use_nulls';

-- A `Distributed` table takes `background_insert_batch` from `distributed_background_insert_batch` where its
-- own `SETTINGS` clause states nothing, so the engine's row has to take it from there too.
SET distributed_background_insert_batch = 0;
SELECT name, value, changed FROM system.engine_settings
WHERE engine = 'Distributed' AND name = 'background_insert_batch';
SET distributed_background_insert_batch = 1;
SELECT name, value, changed, source FROM system.engine_settings
WHERE engine = 'Distributed' AND name = 'background_insert_batch';
