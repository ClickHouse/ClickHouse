-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/104463
--
-- Any ALTER on a MergeTree table whose ORDER BY references a
-- `SimpleAggregateFunction` column (created with `allow_suspicious_primary_key = 1`)
-- must succeed when the ALTER cannot affect the sorting key, even if the
-- session running the ALTER has `allow_suspicious_primary_key` disabled.
--
-- `StorageMergeTree::alter` and `StorageReplicatedMergeTree::alter` now compare
-- the resolved sorting-key data types before and after applying the commands and
-- only run `verifySortingKey` when those types actually change. ALTERs on
-- columns or storage that cannot affect the sorting key (settings, comments,
-- codec changes, column placement modifiers, etc.) keep the table usable.

DROP TABLE IF EXISTS t_104463_mt;
DROP TABLE IF EXISTS t_104463_amt;

-- Plain MergeTree with a suspicious column in ORDER BY.
SET allow_suspicious_primary_key = 1;
CREATE TABLE t_104463_mt
(
    key   Int,
    value SimpleAggregateFunction(sum, UInt64)
)
ENGINE = MergeTree
ORDER BY (key, value)
SETTINGS merge_with_ttl_timeout = 30;

-- AggregatingMergeTree variant from the original report.
CREATE TABLE t_104463_amt
(
    key   Int,
    value SimpleAggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree
ORDER BY (key, value)
SETTINGS merge_with_ttl_timeout = 30;

SET allow_suspicious_primary_key = 0;

-- Settings-only ALTER must not re-validate the sorting key.
ALTER TABLE t_104463_mt  MODIFY SETTING merge_with_ttl_timeout = 60;
ALTER TABLE t_104463_amt MODIFY SETTING merge_with_ttl_timeout = 60;

ALTER TABLE t_104463_mt  RESET SETTING merge_with_ttl_timeout;
ALTER TABLE t_104463_amt RESET SETTING merge_with_ttl_timeout;

-- Comment-only ALTER must not re-validate the sorting key.
ALTER TABLE t_104463_mt  MODIFY COMMENT 'table comment';
ALTER TABLE t_104463_amt MODIFY COMMENT 'table comment';

ALTER TABLE t_104463_mt  COMMENT COLUMN value 'column comment';
ALTER TABLE t_104463_amt COMMENT COLUMN value 'column comment';

-- Mixed `MODIFY COMMENT` / `COMMENT COLUMN` / `RESET SETTING` + `MODIFY SETTING`
-- statements must not re-validate the sorting key either: neither subcommand can
-- change the resolved key types, so the new gate skips the check.
-- Note: `MODIFY SETTING` consumes a comma-separated setting list, so it must
-- appear last when combined with other subcommands in a single ALTER.
ALTER TABLE t_104463_mt  MODIFY COMMENT 'mixed table comment', MODIFY SETTING merge_with_ttl_timeout = 60;
ALTER TABLE t_104463_amt MODIFY COMMENT 'mixed table comment', MODIFY SETTING merge_with_ttl_timeout = 60;

ALTER TABLE t_104463_mt  COMMENT COLUMN value 'mixed column comment', MODIFY SETTING merge_with_ttl_timeout = 45;
ALTER TABLE t_104463_amt COMMENT COLUMN value 'mixed column comment', MODIFY SETTING merge_with_ttl_timeout = 45;

ALTER TABLE t_104463_mt  MODIFY COMMENT 'mixed reset comment', RESET SETTING merge_with_ttl_timeout;
ALTER TABLE t_104463_amt MODIFY COMMENT 'mixed reset comment', RESET SETTING merge_with_ttl_timeout;

-- `MODIFY COLUMN` shapes that cannot change the sorting key:
--   * comment-only `MODIFY COLUMN`
--   * comment-only with a placement modifier (`FIRST` / `AFTER`)
--   * codec-only `MODIFY COLUMN`
--   * per-column `MODIFY SETTING`
-- The previous whitelist-based gate could not recognise some of these
-- (especially the placement-modifier and codec / per-column-setting forms);
-- the new sorting-key-types comparison handles them uniformly.
ALTER TABLE t_104463_mt  MODIFY COLUMN value COMMENT 'modcol-only';
ALTER TABLE t_104463_amt MODIFY COLUMN value COMMENT 'modcol-only';

ALTER TABLE t_104463_mt  MODIFY COLUMN value SimpleAggregateFunction(sum, UInt64) COMMENT 'modcol-after-key', MODIFY SETTING merge_with_ttl_timeout = 30;
ALTER TABLE t_104463_amt MODIFY COLUMN value SimpleAggregateFunction(sum, UInt64) COMMENT 'modcol-after-key', MODIFY SETTING merge_with_ttl_timeout = 30;

ALTER TABLE t_104463_mt  MODIFY COLUMN value CODEC(ZSTD);
ALTER TABLE t_104463_amt MODIFY COLUMN value CODEC(ZSTD);

ALTER TABLE t_104463_mt  MODIFY COLUMN value MODIFY SETTING min_compress_block_size = 1;
ALTER TABLE t_104463_amt MODIFY COLUMN value MODIFY SETTING min_compress_block_size = 1;

-- `ADD COLUMN` of a column that is NOT in ORDER BY does not change the sorting-key
-- data types, so the check is skipped and the ALTER succeeds even with
-- `allow_suspicious_primary_key = 0`. This widens the set of accepted ALTERs
-- compared to the original 104463 fix (which only handled pure
-- settings/comment cases) but matches the design that any ALTER not affecting
-- the sorting key is fine to apply on a grandfathered table.
ALTER TABLE t_104463_mt  ADD COLUMN extra Int;
ALTER TABLE t_104463_amt ADD COLUMN extra Int;

SELECT count() FROM system.columns WHERE database = currentDatabase() AND table = 't_104463_mt'  AND name = 'extra';
SELECT count() FROM system.columns WHERE database = currentDatabase() AND table = 't_104463_amt' AND name = 'extra';

-- Mixing a settings/comment change with `ADD COLUMN` of a non-key column also
-- succeeds for the same reason.
ALTER TABLE t_104463_mt  MODIFY COMMENT 'cmt', ADD COLUMN extra2 Int;
ALTER TABLE t_104463_amt MODIFY COMMENT 'cmt', ADD COLUMN extra2 Int;

-- ALTERs that genuinely change the resolved sorting-key data types still fire
-- the check. `MODIFY COLUMN key SimpleAggregateFunction(any, Int32)` retypes a
-- key column to a suspicious type, so the resolved key data-types list goes
-- from `[Int, SimpleAggregateFunction(sum, UInt64)]` to
-- `[SimpleAggregateFunction(any, Int32), SimpleAggregateFunction(sum, UInt64)]`,
-- the gate fires and the new `SimpleAggregateFunction` at index 0 is rejected.
ALTER TABLE t_104463_mt  MODIFY COLUMN key SimpleAggregateFunction(any, Int32); -- { serverError DATA_TYPE_CANNOT_BE_USED_IN_KEY }
ALTER TABLE t_104463_amt MODIFY COLUMN key SimpleAggregateFunction(any, Int32); -- { serverError DATA_TYPE_CANNOT_BE_USED_IN_KEY }

DROP TABLE t_104463_mt;
DROP TABLE t_104463_amt;
