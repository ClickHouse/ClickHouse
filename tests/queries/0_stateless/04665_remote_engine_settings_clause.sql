SET send_logs_level = 'fatal';

-- The `Remote` and `RemoteSecure` table engines take the settings of the `Distributed` storage they
-- create after the engine definition, unlike the `remote` and `remoteSecure` table functions, which
-- take a `SETTINGS` clause among their arguments.

DROP TABLE IF EXISTS remote_engine_with_settings;

CREATE TABLE remote_engine_with_settings (dummy UInt8) ENGINE = Remote('localhost:1', system, one)
SETTINGS skip_unavailable_shards = 1;

SHOW CREATE TABLE remote_engine_with_settings;

-- The only shard is unavailable, and the setting of the storage makes it skipped instead of failing.
-- A distributed aggregation over no shards returns nothing, so the query below prints no rows.
SELECT count() FROM remote_engine_with_settings;
SELECT 'the shard was skipped';

-- A setting specified in the query has priority over the setting of the storage.
SELECT count() FROM remote_engine_with_settings SETTINGS skip_unavailable_shards = 0; -- { serverError ALL_CONNECTION_TRIES_FAILED }

DETACH TABLE remote_engine_with_settings;
ATTACH TABLE remote_engine_with_settings;

SHOW CREATE TABLE remote_engine_with_settings;
SELECT count() FROM remote_engine_with_settings;
SELECT 'the shard was skipped after ATTACH';

DROP TABLE remote_engine_with_settings;
