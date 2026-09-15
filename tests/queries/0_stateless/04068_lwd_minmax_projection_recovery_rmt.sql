-- Verify that minmax_count_projection is re-enabled after all lightweight-deleted parts are merged away.
-- Previously, the has_lightweight_delete_parts flag was "sticky" and never reset,
-- permanently disabling minmax_count_projection even after OPTIMIZE TABLE FINAL.

SET lightweight_deletes_sync = 2, alter_sync = 2;

DROP TABLE IF EXISTS t_lwd_proj;

-- `number_of_free_entries_in_pool_to_execute_mutation = 0` keeps the lightweight delete below
-- independent of the server-global merges/mutations pool: concurrent tests can occupy enough of
-- it that the default threshold refuses to assign the mutation, and the merge-selecting task then
-- retries only after `max_merge_selecting_sleep_ms`, so `lightweight_deletes_sync = 2` times out.
CREATE TABLE t_lwd_proj
(
    dt Date,
    id UInt64,
    value String
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/{database}/tables/test_cleanup/', '1')
PARTITION BY toYYYYMM(dt)
ORDER BY (id, dt)
SETTINGS number_of_free_entries_in_pool_to_execute_mutation = 0;

INSERT INTO t_lwd_proj VALUES ('2025-01-15', 1, 'First record');
INSERT INTO t_lwd_proj VALUES ('2025-02-15', 2, 'Second record');
INSERT INTO t_lwd_proj VALUES ('2025-03-15', 3, 'Third record');
INSERT INTO t_lwd_proj VALUES ('2025-04-15', 4, 'Fourth record');
INSERT INTO t_lwd_proj VALUES ('2025-05-15', 5, 'Fifth record');
INSERT INTO t_lwd_proj VALUES ('2025-06-15', 6, 'Sixth record');
INSERT INTO t_lwd_proj VALUES ('2025-07-15', 7, 'Seventh record');
INSERT INTO t_lwd_proj VALUES ('2025-08-15', 8, 'Eighth record');
INSERT INTO t_lwd_proj VALUES ('2025-09-15', 9, 'Ninth record');
INSERT INTO t_lwd_proj VALUES ('2025-10-15', 10, 'Tenth record');
INSERT INTO t_lwd_proj VALUES ('2025-11-15', 11, 'Eleventh record');
INSERT INTO t_lwd_proj VALUES ('2025-12-15', 12, 'Twelfth record');

-- Before any delete: minmax_count_projection should be used.
SELECT 'before_delete';
SELECT count() FROM (EXPLAIN indexes = 1 SELECT min(dt), max(dt) FROM t_lwd_proj
    SETTINGS optimize_use_projections = 1, optimize_use_implicit_projections = 1)
WHERE explain LIKE '%_minmax_count_projection%';

-- Perform lightweight delete.
DELETE FROM t_lwd_proj WHERE id < 5;

-- After delete: minmax_count_projection should NOT be used (parts have LWD mask).
SELECT 'after_delete';
SELECT count() FROM (EXPLAIN indexes = 1 SELECT min(dt), max(dt) FROM t_lwd_proj
    SETTINGS optimize_use_projections = 1, optimize_use_implicit_projections = 1)
WHERE explain LIKE '%_minmax_count_projection%';

-- Merge away the LWD parts.
OPTIMIZE TABLE t_lwd_proj FINAL;

-- Verify no parts have lightweight delete mask.
SELECT 'lwd_parts_remaining', countIf(has_lightweight_delete) FROM system.parts
WHERE table = 't_lwd_proj' AND database = currentDatabase() AND active;

-- After merge: minmax_count_projection should be used again.
SELECT 'after_optimize';
SELECT count() FROM (EXPLAIN indexes = 1 SELECT min(dt), max(dt) FROM t_lwd_proj
    SETTINGS optimize_use_projections = 1, optimize_use_implicit_projections = 1)
WHERE explain LIKE '%_minmax_count_projection%';

DROP TABLE t_lwd_proj;
