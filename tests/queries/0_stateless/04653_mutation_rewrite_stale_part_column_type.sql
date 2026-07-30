-- A part can carry an older type for a column than the table metadata does, because an `ALTER TABLE
-- ... MODIFY COLUMN` is not always applied to the data right away. A mutation that rewrites the whole
-- part re-reads every column at the type in the metadata, so the resulting part must record that type
-- and not the stale one from the source part.

SELECT '--- materialize a projection after a type change ---';

DROP TABLE IF EXISTS t_stale_part_type_1;

-- `enable_block_number_column` makes every mutation that goes through the interpreter also materialize
-- `_block_number`, and `auto_statistics_types` keeps the type change itself out of the data rewrite, so
-- the part is still `String` when the projection is materialized.
CREATE TABLE t_stale_part_type_1 (s String) ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0,
         enable_block_number_column = 1, auto_statistics_types = 'tdigest';

INSERT INTO t_stale_part_type_1 SELECT 'str' FROM numbers(1);

ALTER TABLE t_stale_part_type_1 MODIFY COLUMN s Nullable(String) SETTINGS mutations_sync = 1;
ALTER TABLE t_stale_part_type_1 ADD PROJECTION p1 (SELECT s ORDER BY s);
ALTER TABLE t_stale_part_type_1 MATERIALIZE PROJECTION p1 SETTINGS mutations_sync = 1;

SELECT s FROM t_stale_part_type_1;

SELECT column, type FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_stale_part_type_1' AND active AND column = 's';

DROP TABLE t_stale_part_type_1;

SELECT '--- a part detached across the type change ---';

DROP TABLE IF EXISTS t_stale_part_type_2;

CREATE TABLE t_stale_part_type_2 (a String, b String, c String MATERIALIZED concat(a, '!'))
ENGINE = MergeTree ORDER BY a SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;

INSERT INTO t_stale_part_type_2 VALUES ('x', 'y');

ALTER TABLE t_stale_part_type_2 DETACH PART 'all_1_1_0';
ALTER TABLE t_stale_part_type_2 MODIFY COLUMN b Nullable(String);
ALTER TABLE t_stale_part_type_2 ATTACH PART 'all_1_1_0';

SELECT 'part type before the rewrite';
SELECT column, type FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_stale_part_type_2' AND active AND column = 'b';

ALTER TABLE t_stale_part_type_2 ADD PROJECTION p_ab (SELECT a, b ORDER BY a);

-- `MATERIALIZE COLUMN c` writes `c`, the projection needs `a` and `b` to be read, and together they
-- cover every column of the table, so the mutation rewrites the whole part. `b` is only read, not
-- written by a mutation command, so it is absent from the updated header.
ALTER TABLE t_stale_part_type_2 MATERIALIZE COLUMN c, MATERIALIZE PROJECTION p_ab SETTINGS mutations_sync = 1;

SELECT 'part type after the rewrite';
SELECT column, type FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_stale_part_type_2' AND active AND column = 'b';

SELECT 'data';
SELECT a, b, c FROM t_stale_part_type_2 ORDER BY a;

SELECT 'read from the projection';
SELECT a, b FROM t_stale_part_type_2 ORDER BY a SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;

DROP TABLE t_stale_part_type_2;
