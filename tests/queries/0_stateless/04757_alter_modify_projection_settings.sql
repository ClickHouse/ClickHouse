DROP TABLE IF EXISTS t_modify_projection;

CREATE TABLE t_modify_projection
(
    k UInt64,
    v UInt64,
    PROJECTION p (SELECT v ORDER BY v) WITH SETTINGS (index_granularity = 1024)
)
ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 8192, index_granularity_bytes = 10485760;

-- The test asserts that the two inserted parts stay separate until OPTIMIZE, so a
-- spontaneous background merge must not combine them earlier.
SYSTEM STOP MERGES t_modify_projection;

INSERT INTO t_modify_projection SELECT number, number * 2 FROM numbers(10000);

SELECT '-- initial definition';
SHOW CREATE TABLE t_modify_projection;

SELECT '-- modify projection settings';
ALTER TABLE t_modify_projection MODIFY PROJECTION p (SELECT v ORDER BY v) WITH SETTINGS (index_granularity = 128);

SELECT '-- new definition reflects the new setting';
SHOW CREATE TABLE t_modify_projection;

SELECT '-- the old part keeps the old granularity, a new part gets the new one';
INSERT INTO t_modify_projection SELECT number, number * 2 FROM numbers(10000);

-- `name` is the projection name and is the same for both parts, so order by the
-- parent part name to make the output deterministic.
SELECT name, rows, marks
FROM system.projection_parts
WHERE database = currentDatabase() AND table = 't_modify_projection' AND active
ORDER BY parent_name;

SELECT '-- a merge rebuilds the projection with the new granularity';
SYSTEM START MERGES t_modify_projection;
OPTIMIZE TABLE t_modify_projection FINAL;

SELECT name, rows, marks
FROM system.projection_parts
WHERE database = currentDatabase() AND table = 't_modify_projection' AND active
ORDER BY parent_name;

SELECT '-- errors';
ALTER TABLE t_modify_projection MODIFY PROJECTION nonexistent (SELECT v ORDER BY v) WITH SETTINGS (index_granularity = 128); -- { serverError NO_SUCH_PROJECTION_IN_TABLE }
ALTER TABLE t_modify_projection MODIFY PROJECTION IF EXISTS nonexistent (SELECT v ORDER BY v) WITH SETTINGS (index_granularity = 128);
-- IF EXISTS on a missing projection must be a no-op even when the restated definition would not
-- validate (e.g. it references columns dropped together with the projection).
ALTER TABLE t_modify_projection MODIFY PROJECTION IF EXISTS nonexistent (SELECT no_such_column ORDER BY no_such_column) WITH SETTINGS (index_granularity = 128);
ALTER TABLE t_modify_projection MODIFY PROJECTION IF EXISTS nonexistent (SELECT v ORDER BY v) WITH SETTINGS (max_threads = 1);
ALTER TABLE t_modify_projection MODIFY PROJECTION p (SELECT k ORDER BY k) WITH SETTINGS (index_granularity = 128); -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_modify_projection MODIFY PROJECTION p (SELECT v ORDER BY v) WITH SETTINGS (old_parts_lifetime = 100); -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_modify_projection MODIFY PROJECTION p (SELECT v ORDER BY v) WITH SETTINGS (max_threads = 1); -- { serverError UNKNOWN_SETTING }
-- The granularity guard must validate against the post-ALTER settings: a single ALTER may combine
-- a projection granularity override with a switch to fixed granularity, and a settings-only ALTER
-- may switch to fixed granularity under an existing override. Both must be rejected up front.
ALTER TABLE t_modify_projection MODIFY PROJECTION p (SELECT v ORDER BY v) WITH SETTINGS (index_granularity = 256), MODIFY SETTING index_granularity_bytes = 0; -- { serverError SUPPORT_IS_DISABLED }
ALTER TABLE t_modify_projection MODIFY SETTING index_granularity_bytes = 0; -- { serverError SUPPORT_IS_DISABLED }

DROP TABLE t_modify_projection;
