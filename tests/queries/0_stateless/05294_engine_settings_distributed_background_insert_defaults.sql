-- A new `Distributed`, `Remote` or `RemoteSecure` table fills the `background_insert_*` settings its definition leaves
-- out from the `distributed_background_insert_*` settings of the global context, not of the session. So
-- `system.engine_settings` reports these four settings with the server's values, and a `SET` in this session does not
-- move them. `changed` is that of the `distributed` section of the server configuration, which sets none of them here.

-- Before any `SET`, the session's settings are the server's.
SELECT e.engine_name, e.name, e.value = s.value AS server_value, e.changed
FROM system.engine_settings AS e
INNER JOIN system.settings AS s ON s.name = concat('distributed_', e.name)
WHERE e.engine_name IN ('Distributed', 'Remote', 'RemoteSecure') AND e.name LIKE 'background_insert_%'
ORDER BY e.engine_name, e.name;

CREATE TEMPORARY TABLE readings (engine_name String, name String, value String);

SET distributed_background_insert_batch = 0;
INSERT INTO readings SELECT engine_name, name, value FROM system.engine_settings
WHERE engine_name IN ('Distributed', 'Remote', 'RemoteSecure') AND name = 'background_insert_batch';
SET distributed_background_insert_batch = 1;
INSERT INTO readings SELECT engine_name, name, value FROM system.engine_settings
WHERE engine_name IN ('Distributed', 'Remote', 'RemoteSecure') AND name = 'background_insert_batch';

SELECT engine_name, name, uniqExact(value) AS distinct_values, count() AS readings
FROM readings GROUP BY engine_name, name ORDER BY engine_name;
