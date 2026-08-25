-- A table-function target with an explicit column list may refer to an object that exists only on the
-- remote shards, so the initiator's catalog is not authoritative for it. The `timeSeries*` /
-- `prometheusQuery*` family resolves its source table while parsing the arguments and throws
-- `UNEXPECTED_TABLE_ENGINE` when a same-named local table has the wrong engine. The create-time argument
-- validation must defer that failure exactly like the target's absence (`UNKNOWN_TABLE`), otherwise a
-- remote-only definition whose real target lives on the shards is rejected based on irrelevant local state.

SET allow_experimental_time_series_table = 1;

DROP TABLE IF EXISTS ts_shadow_wrong_engine;
DROP TABLE IF EXISTS dist_ts_shadow_remote_only;
DROP TABLE IF EXISTS remote_ts_shadow;
DROP TABLE IF EXISTS dist_ts_shadow_local;
DROP TABLE IF EXISTS dist_ts_shadow_infer;

-- A local table with the target's name but the wrong engine.
CREATE TABLE ts_shadow_wrong_engine (n UInt64) ENGINE = MergeTree ORDER BY n;

-- Over a cluster with no local replicas and with an explicit column list, the wrong-engine local shadow
-- must not reject the definition: the target is resolved only on the shards, at read time.
CREATE TABLE dist_ts_shadow_remote_only
(
    metric_family_name String,
    type LowCardinality(String),
    unit LowCardinality(String),
    help String
)
ENGINE = Distributed(test_cluster_multiple_nodes_all_unavailable, timeSeriesMetrics(ts_shadow_wrong_engine));
SELECT replaceAll(create_table_query, currentDatabase(), 'default') FROM system.tables WHERE database = currentDatabase() AND name = 'dist_ts_shadow_remote_only';

-- The same holds for the `Remote` engine over an address that is not this server.
CREATE TABLE remote_ts_shadow
(
    metric_family_name String,
    type LowCardinality(String),
    unit LowCardinality(String),
    help String
)
ENGINE = Remote('127.0.0.1:1234', timeSeriesMetrics(ts_shadow_wrong_engine));

-- On a cluster with a local shard the explicit-columns definition is still creatable (the target being
-- unresolvable at create time is deferred by design), but a read reaches the local shard and reports the
-- wrong engine.
CREATE TABLE dist_ts_shadow_local
(
    metric_family_name String,
    type LowCardinality(String),
    unit LowCardinality(String),
    help String
)
ENGINE = Distributed(test_shard_localhost, timeSeriesMetrics(ts_shadow_wrong_engine));
SELECT count() FROM dist_ts_shadow_local; -- { serverError UNEXPECTED_TABLE_ENGINE }

-- Without an explicit column list the create-time analysis is the only source of the structure, so the
-- wrong-engine target still fails the `CREATE`.
CREATE TABLE dist_ts_shadow_infer ENGINE = Distributed(test_shard_localhost, timeSeriesMetrics(ts_shadow_wrong_engine)); -- { serverError UNEXPECTED_TABLE_ENGINE }

DROP TABLE dist_ts_shadow_local;
DROP TABLE remote_ts_shadow;
DROP TABLE dist_ts_shadow_remote_only;
DROP TABLE ts_shadow_wrong_engine;
