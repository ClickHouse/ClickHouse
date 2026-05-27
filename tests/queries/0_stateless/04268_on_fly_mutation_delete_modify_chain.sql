-- Lightweight DELETE queued before MODIFY COLUMN: the on-fly chain must apply
-- both the row filter and the type conversion of the column being MODIFY'd, even
-- though MODIFY itself is not applied on-fly (only the version fence is set).
--
-- A previous fix narrowed `columns_overwritten_by_chain` to UPDATE/DELETE targets
-- so the MODIFY'd column is no longer skipped from `performRequiredConversions`.
-- This test guards against re-broadening the set: with the broader set, the
-- output block would carry `c` as String while the schema advertised UInt64.

DROP TABLE IF EXISTS t_on_fly_del_then_mod SYNC;

CREATE TABLE t_on_fly_del_then_mod (id UInt64, c String)
ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO t_on_fly_del_then_mod SELECT number,       toString(number)       FROM numbers(100);
INSERT INTO t_on_fly_del_then_mod SELECT number + 100, toString(number + 100) FROM numbers(100);

SYSTEM STOP MERGES t_on_fly_del_then_mod;

DELETE FROM t_on_fly_del_then_mod WHERE id < 100
    SETTINGS lightweight_deletes_sync = 0, mutations_sync = 0;
ALTER TABLE t_on_fly_del_then_mod MODIFY COLUMN c UInt64
    SETTINGS mutations_sync = 0, alter_sync = 0;

-- Survivors are rows 100..199; column must be UInt64.
SELECT count(), groupUniqArray(toTypeName(c)), min(c), max(c)
FROM t_on_fly_del_then_mod
SETTINGS apply_mutations_on_fly = 1
FORMAT TSV;

DROP TABLE t_on_fly_del_then_mod SYNC;
