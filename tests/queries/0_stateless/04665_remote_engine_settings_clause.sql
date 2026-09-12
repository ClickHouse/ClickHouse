SET send_logs_level = 'fatal';

-- The `Remote` and `RemoteSecure` table engines take the settings of the `Distributed` storage they
-- create after the engine definition, unlike the `remote` and `remoteSecure` table functions, which
-- take a `SETTINGS` clause among their arguments.

DROP TABLE IF EXISTS remote_engine_with_settings;

CREATE TABLE remote_engine_with_settings (dummy UInt8) ENGINE = Remote('localhost:1', system, one)
SETTINGS skip_unavailable_shards = 1;

SHOW CREATE TABLE remote_engine_with_settings;

-- The setting of the storage makes the shard skipped instead of failing. It is the only shard, so
-- nothing is left to answer the query and it fails with ALL_CONNECTION_TRIES_FAILED, the same way
-- 04050 asserts for a Distributed table whose only shard is local. That error code is reached
-- whether the shard was skipped or the setting never applied and the connection simply failed, so
-- the code alone does not say which happened here; `DistributedShardsSkipped` is what says it, and it
-- is asserted for the query below and again for the one after `ATTACH`.
SELECT count() FROM remote_engine_with_settings; -- { serverError ALL_CONNECTION_TRIES_FAILED }
SELECT 'the shard was skipped';

-- A setting specified in the query has priority over the setting of the storage: the shard is not
-- skipped at all, so the failure comes from connecting rather than from having no shard left.
SELECT count() FROM remote_engine_with_settings SETTINGS skip_unavailable_shards = 0; -- { serverError ALL_CONNECTION_TRIES_FAILED }

SYSTEM FLUSH LOGS query_log;

-- The query that overrides the storage setting names it explicitly, so it is the one carrying
-- `skip_unavailable_shards` in `Settings`; the others inherit it from the storage.
SELECT skipped > 0, not_skipped = 0 FROM
(
    SELECT
        maxIf(ProfileEvents['DistributedShardsSkipped'], not has(Settings, 'skip_unavailable_shards')) AS skipped,
        maxIf(ProfileEvents['DistributedShardsSkipped'], Settings['skip_unavailable_shards'] = '0') AS not_skipped
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'ExceptionWhileProcessing'
      AND query LIKE '%FROM remote_engine_with_settings%'
);

DETACH TABLE remote_engine_with_settings;
ATTACH TABLE remote_engine_with_settings;

SHOW CREATE TABLE remote_engine_with_settings;
SELECT count() FROM remote_engine_with_settings SETTINGS log_comment = '04665_after_attach'; -- { serverError ALL_CONNECTION_TRIES_FAILED }
SELECT 'the shard was skipped after ATTACH';

SYSTEM FLUSH LOGS query_log;

-- The query above is textually identical to the one before the DETACH, so it is identified by its
-- `log_comment` rather than its text. Naming `log_comment` does not name `skip_unavailable_shards`,
-- which the query still inherits from the reattached storage, so the skip it reports is the persisted
-- setting being applied.
SELECT skipped > 0 FROM
(
    SELECT max(ProfileEvents['DistributedShardsSkipped']) AS skipped
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'ExceptionWhileProcessing'
      AND log_comment = '04665_after_attach'
);

DROP TABLE remote_engine_with_settings;
