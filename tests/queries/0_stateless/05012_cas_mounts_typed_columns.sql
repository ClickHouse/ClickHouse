-- E3: system.cas_mounts exposes typed columns for the lease identity/timing fields
-- (server_uuid as UUID, started_at/expires_at as DateTime64(3)) instead of raw String/UInt64.
-- No CA disk needs to be mounted -- the table's ColumnsDescription is static.

SELECT type FROM system.columns
WHERE database = 'system' AND table = 'cas_mounts'
  AND name IN ('server_uuid', 'started_at', 'expires_at')
ORDER BY name;
