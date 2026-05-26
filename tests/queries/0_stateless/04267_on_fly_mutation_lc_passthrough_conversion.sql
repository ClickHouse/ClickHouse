-- UPDATE mutations on columns other than `c` queued BEFORE a pending MODIFY COLUMN
-- that wraps `c` in LowCardinality used to leave `c` in the on-disk type while the
-- block advertised the post-MODIFY type. See `MergeTreeReadersChain::executeActionsBeforePrewhere`.

DROP TABLE IF EXISTS t_on_fly_mut_lc SYNC;

CREATE TABLE t_on_fly_mut_lc (id UInt64, name String, c String)
ENGINE = MergeTree ORDER BY id;

INSERT INTO t_on_fly_mut_lc SELECT number, 'n' || toString(number), 's' || toString(number % 3) FROM numbers(50);
INSERT INTO t_on_fly_mut_lc SELECT number + 50, 'n' || toString(number + 50), 's' || toString(number % 3) FROM numbers(50);

SYSTEM STOP MERGES t_on_fly_mut_lc;

ALTER TABLE t_on_fly_mut_lc UPDATE name = concat('u_', name) WHERE 1=1 SETTINGS mutations_sync = 0, alter_sync = 0;
ALTER TABLE t_on_fly_mut_lc UPDATE name = concat('uu_', name) WHERE name LIKE 'u_%' SETTINGS mutations_sync = 0, alter_sync = 0;
ALTER TABLE t_on_fly_mut_lc MODIFY COLUMN c LowCardinality(String) SETTINGS mutations_sync = 0, alter_sync = 0;

-- Used to throw Code 44 `Expected ColumnLowCardinality, got String` in MergingSortedTransform.
SELECT count(), groupUniqArray(toTypeName(c))
FROM t_on_fly_mut_lc
SETTINGS apply_mutations_on_fly = 1
FORMAT TSV;

-- Used to throw Code 49 `Bad cast from DB::ColumnString to DB::ColumnLowCardinality`
-- in `NativeWriter` on the way out.
SELECT count() FROM (
    SELECT id, name, c
    FROM t_on_fly_mut_lc
    ORDER BY id DESC, c
    LIMIT 5000
    SETTINGS apply_mutations_on_fly = 1
);

DROP TABLE t_on_fly_mut_lc;
