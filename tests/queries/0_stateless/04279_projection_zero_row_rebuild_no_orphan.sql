SET lightweight_deletes_sync = 2;
SET mutations_sync = 2;
SET alter_sync = 2;

CREATE TABLE t
(
    x UInt64,
    y UInt64,
    PROJECTION p (SELECT y, count() GROUP BY y)
)
ENGINE = MergeTree
ORDER BY x
SETTINGS min_bytes_for_wide_part = 0,
         lightweight_mutation_projection_mode = 'rebuild';

INSERT INTO t SELECT number, toUInt64(200) FROM numbers(20);

DELETE FROM t WHERE y > 100;

SELECT 'after_delete_broken_projection_parts';
SELECT count() FROM system.projection_parts
WHERE database = currentDatabase() AND table = 't' AND active AND is_broken;

DETACH TABLE t;
ATTACH TABLE t;

SELECT 'after_reload_broken_projection_parts';
SELECT count() FROM system.projection_parts
WHERE database = currentDatabase() AND table = 't' AND active AND is_broken;

SELECT 'visible_rows';
SELECT count() FROM t;
