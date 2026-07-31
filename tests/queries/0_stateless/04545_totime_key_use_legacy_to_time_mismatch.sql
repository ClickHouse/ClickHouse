-- A MergeTree PRIMARY KEY / ORDER BY over toTime(...) must keep the same physical type regardless of
-- the writing session's use_legacy_to_time setting (otherwise the primary-index write hits a bad-cast
-- logical-error exception). See the PR description for the mechanism.

SET allow_experimental_time_time64_type = 1;

DROP TABLE IF EXISTS t_totime_pk;
DROP TABLE IF EXISTS t_totime_order;
DROP TABLE IF EXISTS t_totime_legacy_created;

-- PRIMARY KEY toTime(...): insert with use_legacy_to_time = 1 must not abort.
CREATE TABLE t_totime_pk (c0 Int32, c1 DateTime64 MATERIALIZED nowInBlock64())
ENGINE = MergeTree() PRIMARY KEY (toTime(c1));
INSERT INTO t_totime_pk (c0) SETTINGS use_legacy_to_time = 1 SELECT number FROM numbers(1000);
SELECT count(), uniqExact(c0) FROM t_totime_pk;
SELECT count() FROM t_totime_pk WHERE c0 < 500;

-- ORDER BY toTime(...): same, in both compact and wide parts.
CREATE TABLE t_totime_order (c0 Int32, c1 DateTime64 MATERIALIZED nowInBlock64())
ENGINE = MergeTree() ORDER BY toTime(c1) SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_totime_order (c0) SETTINGS use_legacy_to_time = 1 SELECT number FROM numbers(1000);
SELECT count() FROM t_totime_order;

-- DDL behaviour change carried by this fix: the stored key type is resolved under the server-wide
-- default of use_legacy_to_time, not the writing session's value, so setting it in the session at
-- CREATE time no longer flips the stored key type. DESCRIBE mergeTreeIndex reads the resolved key
-- type back: it is Time (the server default resolution), not DateTime, despite the session setting.
SET use_legacy_to_time = 1;
CREATE TABLE t_totime_legacy_created (c0 Int32, c1 DateTime64 MATERIALIZED nowInBlock64())
ENGINE = MergeTree() PRIMARY KEY (toTime(c1));
SET use_legacy_to_time = 0;
INSERT INTO t_totime_legacy_created (c0) SELECT number FROM numbers(1000);
SELECT count() FROM t_totime_legacy_created;
SET describe_compact_output = 1;
DESCRIBE mergeTreeIndex(currentDatabase(), t_totime_legacy_created);

-- The user-facing toTime type is still governed by the setting in normal query resolution.
SELECT toTypeName(toTime(now64())) SETTINGS use_legacy_to_time = 0;
SELECT toTypeName(toTime(now64())) SETTINGS use_legacy_to_time = 1;

DROP TABLE t_totime_pk;
DROP TABLE t_totime_order;
DROP TABLE t_totime_legacy_created;
