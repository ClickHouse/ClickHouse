-- A CREATE TABLE issued with use_legacy_to_time persists the explicit legacy function name, so
-- MergeTree key expressions do not change their physical type when a later session uses the default.

SET allow_experimental_time_time64_type = 1;

DROP TABLE IF EXISTS t_totime_legacy_created;

SET use_legacy_to_time = 1;
CREATE TABLE t_totime_legacy_created (c0 Int32, c1 DateTime64 MATERIALIZED nowInBlock64())
ENGINE = MergeTree() ORDER BY toTime(c1) SETTINGS min_bytes_for_wide_part = 0;
SET use_legacy_to_time = 0;
INSERT INTO t_totime_legacy_created (c0) SELECT number FROM numbers(1000);
SELECT count() FROM t_totime_legacy_created;
SET describe_compact_output = 1;
DESCRIBE mergeTreeIndex(currentDatabase(), t_totime_legacy_created);

-- The user-facing function remains governed by the setting in ordinary queries.
SELECT toTypeName(toTime(now64())) SETTINGS use_legacy_to_time = 0;
SELECT toTypeName(toTime(now64())) SETTINGS use_legacy_to_time = 1;

DROP TABLE t_totime_legacy_created;
