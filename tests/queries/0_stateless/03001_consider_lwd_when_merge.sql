DROP TABLE IF EXISTS lwd_merge;

CREATE TABLE lwd_merge (id UInt64 CODEC(NONE))
    ENGINE = MergeTree ORDER BY id
SETTINGS max_bytes_to_merge_at_min_space_in_pool = 80000, max_bytes_to_merge_at_max_space_in_pool = 80000, exclude_deleted_rows_for_part_size_in_merge = 0;

INSERT INTO lwd_merge SELECT number FROM numbers(10000);
INSERT INTO lwd_merge SELECT number FROM numbers(10000, 10000);

SET optimize_throw_if_noop = 1;

OPTIMIZE TABLE lwd_merge; -- { serverError CANNOT_ASSIGN_OPTIMIZE }
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 'lwd_merge' AND active = 1;

DELETE FROM lwd_merge WHERE id % 10 > 0;

OPTIMIZE TABLE lwd_merge; -- { serverError CANNOT_ASSIGN_OPTIMIZE }
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 'lwd_merge' AND active = 1;

ALTER TABLE lwd_merge MODIFY SETTING exclude_deleted_rows_for_part_size_in_merge = 1;

-- delete again because deleted rows will be counted in mutation
DELETE FROM lwd_merge WHERE id % 100 == 0;

OPTIMIZE TABLE lwd_merge;
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 'lwd_merge' AND active = 1;

DROP TABLE IF EXISTS lwd_merge;
