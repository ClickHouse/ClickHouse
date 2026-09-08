#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: the `TimeSeries` engine is disabled in the fast-test build.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
set -e

schemas=(
    "version = 0, recent_samples_ttl_seconds = 0
TAGS INNER COLUMNS (id UUID DEFAULT reinterpretAsUUID(sipHash128(metric_name, all_tags)), all_tags Map(String, String))
SAMPLES INNER ENGINE = MergeTree ORDER BY (id, timestamp)"
    "recent_samples_ttl_seconds = 345600
SAMPLES INNER ENGINE = MergeTree ORDER BY (id, timestamp) SETTINGS index_granularity = 32768"
    "recent_samples_ttl_seconds = 0
SAMPLES INNER ENGINE = MergeTree ORDER BY (id.1, id.2, timestamp)"
    "recent_samples_ttl_seconds = 0
SAMPLES INNER ENGINE = MergeTree PARTITION BY toYYYYMM(timestamp) ORDER BY (timestamp, id)"
    "recent_samples_ttl_seconds = 0
SAMPLES INNER ENGINE = MergeTree ORDER BY tuple()"
    "recent_samples_ttl_seconds = 0
TAGS INNER COLUMNS (id UInt64 DEFAULT sipHash64(metric_name, tags))
SAMPLES INNER ENGINE = MergeTree ORDER BY timestamp"
)
names=(uuid tuple tuple_elements time_order unordered scalar)
for i in "${!schemas[@]}"; do
    $CLICKHOUSE_CLIENT --multiquery <<SQL
SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';
SET max_threads = 1;
SET max_block_size = 256;
SET optimize_aggregation_in_order = 0;
SET query_plan_join_swap_table = 1;
SET allow_aggregate_partitions_independently = 1;
SET force_aggregate_partitions_independently = 1;
SET query_plan_enable_optimizations = 1;
SELECT '${names[i]}';
DROP TABLE IF EXISTS ts_stream_schema;
CREATE TABLE ts_stream_schema ENGINE = TimeSeries SETTINGS ${schemas[i]};

-- Two parts in different months: fragments stay bounded, full reads preserve every sample.
INSERT INTO ts_stream_schema (metric_name, tags, time_series, metric_family, type, unit, help)
SELECT 'm', map('n', 'a'),
    arrayMap(i -> (fromUnixTimestamp64Milli(1704067200000 + i), toFloat64(i)), range(32768)),
    'm', 'gauge', 'seconds', 'description';
INSERT INTO ts_stream_schema (metric_name, tags, time_series)
SELECT 'm', map('n', 'a'),
    arrayMap(i -> (fromUnixTimestamp64Milli(1706745600000 + i), toFloat64(i)), range(32768));

-- Each fragment fits a source block; both joins must keep samples on the probe side.
SELECT length(time_series) BETWEEN 1 AND 32768, metric_name, tags['n'], metric_family, type, unit, help
FROM ts_stream_schema LIMIT 1;
SELECT length(time_series) BETWEEN 1 AND 32768 FROM ts_stream_schema LIMIT 1;

SELECT sum(length(time_series)), sum(arraySum(arrayMap(s -> s.2, time_series))),
    min(arrayAll(s -> toUnixTimestamp64Milli(s.1) % 100000 = s.2, time_series))
FROM ts_stream_schema;
SELECT metric_name, tags['n'], length(time_series), arraySum(arrayMap(s -> s.2, time_series))
FROM ts_stream_schema FINAL;

DROP TABLE ts_stream_schema;
SQL
done

# Preserve target-view aggregation, orphan/metadata-only rows, and spilling joins.
$CLICKHOUSE_CLIENT --multiquery <<'SQL'
SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';
SET max_threads = 1;

DROP TABLE IF EXISTS ts_stream_external;
DROP TABLE IF EXISTS ts_stream_samples_view;
DROP TABLE IF EXISTS ts_stream_samples_data;
DROP TABLE IF EXISTS ts_stream_tags;
DROP TABLE IF EXISTS ts_stream_metrics;

CREATE TABLE ts_stream_samples_data (id String, timestamp DateTime64(9), value Float32) ENGINE = Memory;
CREATE VIEW ts_stream_samples_view AS
SELECT id, timestamp, sum(value) AS value FROM ts_stream_samples_data GROUP BY id, timestamp;
CREATE TABLE ts_stream_tags (id String, metric_name String, tags Map(String, String)) ENGINE = Memory;
CREATE TABLE ts_stream_metrics (metric_family_name String, type String, unit String, help String) ENGINE = Memory;
CREATE TABLE ts_stream_external ENGINE = TimeSeries
SETTINGS recent_samples_ttl_seconds = 0, store_min_time_and_max_time = 0
SAMPLES ts_stream_samples_view TAGS ts_stream_tags METRICS ts_stream_metrics;

SELECT '-- empty samples';
SELECT sum(length(time_series)) FROM ts_stream_external;

-- The aggregation in the target view must remain a full aggregation. Only the generated
-- samples aggregation is changed to assemble independent blocks.
INSERT INTO ts_stream_samples_data VALUES ('a', 1, 1), ('b', 2, 2), ('orphan', 3, 100);
INSERT INTO ts_stream_samples_data VALUES ('a', 1, 10), ('b', 2, 20);
INSERT INTO ts_stream_tags VALUES ('a', 'm', {'n': 'a'}), ('b', 'm', {'n': 'b'}), ('b', 'm', {'n': 'b'});
INSERT INTO ts_stream_tags VALUES ('b', 'm', {'n': 'b'});
INSERT INTO ts_stream_metrics VALUES ('m', 'gauge', 'seconds', 'description'), ('unused', 'gauge', '', 'no samples');

SELECT '-- samples-only reads retain orphan samples';
SELECT arraySort(arrayFlatten(groupArray(time_series))) FROM ts_stream_external;

SELECT '-- joins retain metadata-only rows and deduplicate tags';
SELECT metric_name, tags['n'], arraySort(arrayFlatten(groupArray(time_series))), metric_family, type, unit, help
FROM ts_stream_external
GROUP BY ALL ORDER BY metric_family, tags['n'];

SELECT '-- caller aggregation and filters still apply across all fragments';
SELECT sum(arraySum(arrayMap(s -> s.2, time_series))) FROM ts_stream_external WHERE tags['n'] = 'b';
SELECT metric_name, tags['n'], arraySort(time_series), metric_family
FROM ts_stream_external FINAL ORDER BY metric_family, tags['n'];

-- A low spill threshold must still be honored by the internal hash joins.
SELECT '-- disk-backed joins';
SELECT metric_name, tags['n'], arraySort(time_series), metric_family
FROM ts_stream_external ORDER BY metric_family, tags['n']
SETTINGS max_bytes_before_external_join = 1, grace_hash_join_initial_buckets = 2;

DROP TABLE ts_stream_external;
DROP TABLE ts_stream_samples_view;
DROP TABLE ts_stream_samples_data;
DROP TABLE ts_stream_tags;
DROP TABLE ts_stream_metrics;
SQL
