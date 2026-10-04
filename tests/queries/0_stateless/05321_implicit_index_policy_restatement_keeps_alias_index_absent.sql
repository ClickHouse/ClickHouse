-- A settings-only `ALTER` that restates the read-only implicit-index policy with the value it already has
-- must not re-create an implicit index that `REMOVE ALIAS` deliberately dropped: the existing parts still
-- hold its files built over the alias expression, and re-creating it would prune against them.

DROP TABLE IF EXISTS t_policy_restatement;

CREATE TABLE t_policy_restatement (x UInt64, a UInt64 ALIAS x * 2)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS add_minmax_index_for_numeric_columns = 1;

INSERT INTO t_policy_restatement SELECT number FROM numbers(10);

SELECT 'initial', name FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 't_policy_restatement' ORDER BY name;

ALTER TABLE t_policy_restatement MODIFY COLUMN a REMOVE ALIAS;
SELECT 'after REMOVE ALIAS', name FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 't_policy_restatement' ORDER BY name;

ALTER TABLE t_policy_restatement MODIFY SETTING add_minmax_index_for_numeric_columns = 1;
SELECT 'after restatement', name FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 't_policy_restatement' ORDER BY name;

-- The column now reads its physical default for the existing rows.
SELECT count() FROM t_policy_restatement WHERE a = 0;
SELECT count() FROM t_policy_restatement WHERE a > 5;

ALTER TABLE t_policy_restatement MODIFY SETTING add_minmax_index_for_numeric_columns = 0; -- { serverError READONLY_SETTING }

DROP TABLE t_policy_restatement;
