-- UPDATE mutations on columns other than `c` queued BEFORE a pending MODIFY COLUMN
-- that wraps `c` in LowCardinality used to leave `c` in the on-disk type while the
-- block advertised the post-MODIFY type. See `MergeTreeReadersChain::executeActionsBeforePrewhere`.
--
-- The bug fires when:
--   - a sort over the to-be-LC column forces it through `MergingSortedTransform` instead
--     of lazy materialization (`query_plan_max_limit_for_lazy_materialization = 10` keeps
--     lazy materialization off for the `LIMIT 5000` here, matching the production setting);
--   - `apply_mutations_on_fly = 1` engages the on-fly mutation step that previously
--     skipped `performRequiredConversions` for the column.

DROP TABLE IF EXISTS t_on_fly_mut_lc SYNC;

CREATE TABLE t_on_fly_mut_lc (id UInt64, name String, c String)
ENGINE = MergeTree ORDER BY id;

INSERT INTO t_on_fly_mut_lc SELECT number,        'n' || toString(number),        's' || toString(number % 5) FROM numbers(100);
INSERT INTO t_on_fly_mut_lc SELECT number + 100,  'n' || toString(number + 100),  's' || toString(number % 5) FROM numbers(100);

SYSTEM STOP MERGES t_on_fly_mut_lc;

-- UPDATE BEFORE the MODIFY COLUMN — the on-fly mutation step then has
-- `perform_alter_conversions = false`.
ALTER TABLE t_on_fly_mut_lc UPDATE name = concat('u_', name) WHERE 1 = 1
    SETTINGS mutations_sync = 0, alter_sync = 0;
ALTER TABLE t_on_fly_mut_lc MODIFY COLUMN c LowCardinality(String)
    SETTINGS mutations_sync = 0, alter_sync = 0;

-- Used to throw `Code 44. Expected ColumnLowCardinality, got String:
-- While executing MergingSortedTransform`. After the fix it returns 200 rows.
SELECT name, c FROM t_on_fly_mut_lc
ORDER BY c
LIMIT 5000
SETTINGS apply_mutations_on_fly = 1, query_plan_max_limit_for_lazy_materialization = 10
FORMAT Null;

-- Sanity: column is correctly typed for the user.
SELECT count(), groupUniqArray(toTypeName(c))
FROM t_on_fly_mut_lc
SETTINGS apply_mutations_on_fly = 1
FORMAT TSV;

DROP TABLE t_on_fly_mut_lc;
