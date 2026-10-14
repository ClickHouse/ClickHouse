DROP TABLE IF EXISTS mutation_copy_projection;

-- On object-storage disks, independent file copies must see the destination
-- directories committed by `MutateTask`, including the unchanged projection.
CREATE TABLE mutation_copy_projection
(
    key UInt32,
    value UInt32,
    unchanged String,
    PROJECTION p (SELECT unchanged ORDER BY unchanged)
)
ENGINE = MergeTree ORDER BY key
SETTINGS always_use_copy_instead_of_hardlinks = 1,
         min_bytes_for_full_part_storage = 0,
         min_rows_for_wide_part = 0,
         min_bytes_for_wide_part = 0;

INSERT INTO mutation_copy_projection VALUES (1, 10, 'a'), (2, 20, 'b');

ALTER TABLE mutation_copy_projection UPDATE value = value + 1 WHERE key = 1
SETTINGS mutations_sync = 1;

SELECT key, value, unchanged FROM mutation_copy_projection ORDER BY key;
SELECT unchanged FROM mutation_copy_projection ORDER BY unchanged
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1, optimize_read_in_order = 0;

DROP TABLE mutation_copy_projection;
