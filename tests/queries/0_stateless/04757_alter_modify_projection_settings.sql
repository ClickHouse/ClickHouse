DROP TABLE IF EXISTS t_modify_projection;

CREATE TABLE t_modify_projection
(
    k UInt64,
    v UInt64,
    PROJECTION p (SELECT v ORDER BY v) WITH SETTINGS (index_granularity = 1024)
)
ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 8192, index_granularity_bytes = 10485760;

INSERT INTO t_modify_projection SELECT number, number * 2 FROM numbers(10000);

SELECT '-- initial definition';
SHOW CREATE TABLE t_modify_projection;

SELECT '-- modify projection settings';
ALTER TABLE t_modify_projection MODIFY PROJECTION p (SELECT v ORDER BY v) WITH SETTINGS (index_granularity = 128);

SELECT '-- new definition reflects the new setting';
SHOW CREATE TABLE t_modify_projection;

SELECT '-- the old part keeps the old granularity, a new part gets the new one';
INSERT INTO t_modify_projection SELECT number, number * 2 FROM numbers(10000);

SELECT name, rows, marks
FROM system.projection_parts
WHERE database = currentDatabase() AND table = 't_modify_projection' AND active
ORDER BY name;

SELECT '-- a merge rebuilds the projection with the new granularity';
OPTIMIZE TABLE t_modify_projection FINAL;

SELECT name, rows, marks
FROM system.projection_parts
WHERE database = currentDatabase() AND table = 't_modify_projection' AND active
ORDER BY name;

SELECT '-- errors';
ALTER TABLE t_modify_projection MODIFY PROJECTION nonexistent (SELECT v ORDER BY v) WITH SETTINGS (index_granularity = 128); -- { serverError NO_SUCH_PROJECTION_IN_TABLE }
ALTER TABLE t_modify_projection MODIFY PROJECTION IF EXISTS nonexistent (SELECT v ORDER BY v) WITH SETTINGS (index_granularity = 128);
ALTER TABLE t_modify_projection MODIFY PROJECTION p (SELECT k ORDER BY k) WITH SETTINGS (index_granularity = 128); -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_modify_projection MODIFY PROJECTION p (SELECT v ORDER BY v) WITH SETTINGS (old_parts_lifetime = 100); -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_modify_projection MODIFY PROJECTION p (SELECT v ORDER BY v) WITH SETTINGS (max_threads = 1); -- { serverError UNKNOWN_SETTING }

DROP TABLE t_modify_projection;
