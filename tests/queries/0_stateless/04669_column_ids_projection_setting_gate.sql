-- Tags: no-parallel-replicas
-- why: `serialization_info_version` is settable per projection, so a projection could declare
-- `with_column_ids` on a table that has no column-ID mapping -- claiming a format its table does
-- not have, and reaching it without the `allow_experimental_column_ids` gate.
-- no-parallel-replicas: force_optimize_projection below.

CREATE TABLE t_proj_ids_no_mapping (a UInt64, b String, c UInt64,
    PROJECTION p (SELECT a, sum(c) GROUP BY a) WITH SETTINGS (serialization_info_version = 'with_column_ids'))
ENGINE = MergeTree ORDER BY a SETTINGS min_bytes_for_wide_part = 0; -- { serverError SUPPORT_IS_DISABLED }

CREATE TABLE t_proj_ids_added (a UInt64, c UInt64) ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_wide_part = 0;
ALTER TABLE t_proj_ids_added ADD PROJECTION p (SELECT a, sum(c) GROUP BY a)
    WITH SETTINGS (serialization_info_version = 'with_column_ids'); -- { serverError SUPPORT_IS_DISABLED }
DROP TABLE t_proj_ids_added SYNC;

-- why: a projection of an ID table inherits `with_column_ids` and must keep working, including
-- across a rename of a column it reads.
SET allow_experimental_column_ids = 1;

CREATE TABLE t_proj_ids_inherited (a UInt64, b String, c UInt64,
    PROJECTION p (SELECT a, sum(c) GROUP BY a))
ENGINE = MergeTree ORDER BY a
SETTINGS serialization_info_version = 'with_column_ids', min_bytes_for_wide_part = 0;
INSERT INTO t_proj_ids_inherited VALUES (1, 'one', 10), (1, 'two', 20), (2, 'three', 30);
SELECT 'inherited', a, sum(c) FROM t_proj_ids_inherited GROUP BY a ORDER BY a
SETTINGS force_optimize_projection = 1;
ALTER TABLE t_proj_ids_inherited RENAME COLUMN b TO d;
SELECT 'inherited_after_rename', a, sum(c) FROM t_proj_ids_inherited GROUP BY a ORDER BY a
SETTINGS force_optimize_projection = 1;
DROP TABLE t_proj_ids_inherited SYNC;

-- why: the same setting spelled out on a projection of an ID table agrees with the table, so it stays allowed.
CREATE TABLE t_proj_ids_explicit (a UInt64, c UInt64,
    PROJECTION p (SELECT a, sum(c) GROUP BY a) WITH SETTINGS (serialization_info_version = 'with_column_ids'))
ENGINE = MergeTree ORDER BY a
SETTINGS serialization_info_version = 'with_column_ids', min_bytes_for_wide_part = 0;
INSERT INTO t_proj_ids_explicit VALUES (1, 10), (2, 20);
SELECT 'explicit', a, sum(c) FROM t_proj_ids_explicit GROUP BY a ORDER BY a
SETTINGS force_optimize_projection = 1;
DROP TABLE t_proj_ids_explicit SYNC;
