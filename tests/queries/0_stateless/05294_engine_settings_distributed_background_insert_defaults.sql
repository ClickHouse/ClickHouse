-- A new `Distributed`, `Remote` or `RemoteSecure` table fills the `background_insert_*` settings its definition leaves
-- out from the `distributed_background_insert_*` settings of the global context, not of the session. So
-- `system.engine_settings` reports these four settings as filled in, and a `SET` in this session does not move them.

CREATE TEMPORARY TABLE readings (engine_name String, name String, value String);

SET distributed_background_insert_batch = 0;
INSERT INTO readings SELECT engine_name, name, value FROM system.engine_settings
WHERE engine_name IN ('Distributed', 'Remote', 'RemoteSecure') AND name = 'background_insert_batch';
SET distributed_background_insert_batch = 1;
INSERT INTO readings SELECT engine_name, name, value FROM system.engine_settings
WHERE engine_name IN ('Distributed', 'Remote', 'RemoteSecure') AND name = 'background_insert_batch';

SELECT engine_name, name, uniqExact(value) AS distinct_values, count() AS readings
FROM readings GROUP BY engine_name, name ORDER BY engine_name;

-- Filled in: `changed`, or a value other than the compiled default. The `Milliseconds` two take the `changed` flag of the
-- core setting along with its value, so they need the second test.
SELECT engine_name, name, changed OR value != `default` AS filled FROM system.engine_settings
WHERE engine_name IN ('Distributed', 'Remote', 'RemoteSecure') AND name LIKE 'background_insert_%'
ORDER BY engine_name, name;
