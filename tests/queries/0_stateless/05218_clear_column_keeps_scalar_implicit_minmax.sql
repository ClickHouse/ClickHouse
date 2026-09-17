-- CLEAR COLUMN and CLEAR COLUMN IN PARTITION keep the column definition, so they must also
-- keep `auto_minmax_index_<column>`. `AlterCommand::apply` used to drop the exact-column
-- implicit index before the `clear` / `partition` guard; `tryConvertToMutationCommand` calls
-- `apply` while building the committed metadata, so a scalar `CLEAR COLUMN x` silently lost
-- `auto_minmax_index_x`. Nested-parent CLEAR does not expose that: there is no exact
-- `auto_minmax_index_n` to remove.

DROP TABLE IF EXISTS t_scalar_clear_keeps_implicit;
CREATE TABLE t_scalar_clear_keeps_implicit (x Int64, y Int64)
    ENGINE = MergeTree ORDER BY tuple()
    SETTINGS add_minmax_index_for_numeric_columns = 1;

INSERT INTO t_scalar_clear_keeps_implicit VALUES (1, 2);

SELECT 'before', name FROM system.data_skipping_indices
    WHERE database = currentDatabase() AND table = 't_scalar_clear_keeps_implicit' ORDER BY name;

ALTER TABLE t_scalar_clear_keeps_implicit CLEAR COLUMN x SETTINGS mutations_sync = 2;
SELECT 'after clear', name FROM system.data_skipping_indices
    WHERE database = currentDatabase() AND table = 't_scalar_clear_keeps_implicit' ORDER BY name;
SELECT x, y FROM t_scalar_clear_keeps_implicit;

DROP TABLE t_scalar_clear_keeps_implicit;

DROP TABLE IF EXISTS t_scalar_clear_in_partition_keeps_implicit;
CREATE TABLE t_scalar_clear_in_partition_keeps_implicit (x Int64, y Int64)
    ENGINE = MergeTree ORDER BY tuple()
    SETTINGS add_minmax_index_for_numeric_columns = 1;

INSERT INTO t_scalar_clear_in_partition_keeps_implicit VALUES (3, 4);

ALTER TABLE t_scalar_clear_in_partition_keeps_implicit CLEAR COLUMN x IN PARTITION tuple()
    SETTINGS mutations_sync = 2;
SELECT 'after clear in partition', name FROM system.data_skipping_indices
    WHERE database = currentDatabase() AND table = 't_scalar_clear_in_partition_keeps_implicit' ORDER BY name;
SELECT x, y FROM t_scalar_clear_in_partition_keeps_implicit;

DROP TABLE t_scalar_clear_in_partition_keeps_implicit;
