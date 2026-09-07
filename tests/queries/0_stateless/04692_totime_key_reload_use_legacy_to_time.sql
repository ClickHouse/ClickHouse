-- The physical type of a stored `toTime(...)` key expression must be stable across a metadata reload
-- (`DETACH` / `ATTACH`, the same path a server restart takes to rebuild the key from the stored AST)
-- and must not depend on the value of `use_legacy_to_time` in the session performing the reload.

SET allow_experimental_time_time64_type = 1;
SET describe_compact_output = 1;

DROP TABLE IF EXISTS t_totime_reload;

SET use_legacy_to_time = 1;
CREATE TABLE t_totime_reload (c0 Int32, c1 DateTime MATERIALIZED toDateTime('2024-01-01 12:34:56') + c0)
ENGINE = MergeTree() PRIMARY KEY (toTime(c1));
INSERT INTO t_totime_reload (c0) SELECT number FROM numbers(1000);
DESCRIBE mergeTreeIndex(currentDatabase(), t_totime_reload);
SELECT count() FROM t_totime_reload;
SELECT count() FROM t_totime_reload WHERE c1 < toDateTime('2024-01-01 12:40:00');

-- Reload the metadata with the opposite value of the setting: the resolved key type must not move.
DETACH TABLE t_totime_reload;
SET use_legacy_to_time = 0;
ATTACH TABLE t_totime_reload;
DESCRIBE mergeTreeIndex(currentDatabase(), t_totime_reload);
SELECT count() FROM t_totime_reload;
SELECT count() FROM t_totime_reload WHERE c1 < toDateTime('2024-01-01 12:40:00');

-- New parts written after the reload must keep matching the key type of the existing ones.
INSERT INTO t_totime_reload (c0) SELECT number FROM numbers(1000, 1000);
OPTIMIZE TABLE t_totime_reload FINAL;
SELECT count() FROM t_totime_reload;
SELECT count() FROM t_totime_reload WHERE c1 < toDateTime('2024-01-01 12:40:00');

-- And once more with the legacy value set in the reattaching session.
DETACH TABLE t_totime_reload;
SET use_legacy_to_time = 1;
ATTACH TABLE t_totime_reload;
DESCRIBE mergeTreeIndex(currentDatabase(), t_totime_reload);
SELECT count() FROM t_totime_reload;
SELECT count() FROM t_totime_reload WHERE c1 < toDateTime('2024-01-01 12:40:00');

DROP TABLE t_totime_reload;
