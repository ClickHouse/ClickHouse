-- Every `ALTER` payload that reaches table metadata must canonicalize a legacy `toTime` spelling
-- the same way `CREATE` does under `use_legacy_to_time`, so that a metadata reload (`DETACH` /
-- `ATTACH`, the restart path) re-derives the semantics the statement resolved instead of the
-- server default.

SET allow_experimental_time_time64_type = 1;

DROP TABLE IF EXISTS mv_totime_alter;
DROP TABLE IF EXISTS t_totime_alter;

SET use_legacy_to_time = 1;

CREATE TABLE t_totime_alter (c0 DateTime('UTC'), c1 UInt32) ENGINE = MergeTree ORDER BY c1;

-- Each family of persisted `ALTER` payloads, spelled with the legacy `toTime`.
ALTER TABLE t_totime_alter ADD COLUMN d DateTime('UTC') DEFAULT toTime(c0);
ALTER TABLE t_totime_alter ADD COLUMN e DateTime('UTC') DEFAULT c0 TTL toTime(c0) + INTERVAL 100 YEAR;
ALTER TABLE t_totime_alter ADD CONSTRAINT c_time CHECK toUInt32(toTime(c0)) < 200000;
ALTER TABLE t_totime_alter ADD INDEX i_time (toUInt32(toTime(c0))) TYPE minmax GRANULARITY 1;
ALTER TABLE t_totime_alter ADD PROJECTION p_time (SELECT c1 ORDER BY toUInt32(toTime(c0)));

CREATE MATERIALIZED VIEW mv_totime_alter ENGINE = MergeTree ORDER BY tuple() AS SELECT toTime(c0) AS t FROM t_totime_alter;
ALTER TABLE mv_totime_alter MODIFY QUERY SELECT toTime(c0) AS t FROM t_totime_alter;

-- The stored DDL must spell every payload canonically; no raw `toTime(` may survive.
SELECT 'table', countSubstrings(create_table_query, 'toTimeWithFixedDate('), countSubstrings(create_table_query, 'toTime(')
FROM system.tables WHERE database = currentDatabase() AND name = 't_totime_alter';
SELECT 'view', countSubstrings(create_table_query, 'toTimeWithFixedDate('), countSubstrings(create_table_query, 'toTime(')
FROM system.tables WHERE database = currentDatabase() AND name = 'mv_totime_alter';

INSERT INTO t_totime_alter (c0, c1) VALUES ('2026-01-02 03:04:05', 1);

-- Reload the metadata with the opposite value of the setting: the same insert must keep
-- producing the same values, and the table must still load (a raw `toTime` column TTL would
-- not even resolve any more).
DETACH TABLE mv_totime_alter;
DETACH TABLE t_totime_alter;
SET use_legacy_to_time = 0;
ATTACH TABLE t_totime_alter;
ATTACH TABLE mv_totime_alter;

INSERT INTO t_totime_alter (c0, c1) VALUES ('2026-01-02 03:04:05', 2);

SELECT c1, d, e FROM t_totime_alter ORDER BY c1;
SELECT t FROM mv_totime_alter ORDER BY ALL;

DROP TABLE mv_totime_alter;
DROP TABLE t_totime_alter;
