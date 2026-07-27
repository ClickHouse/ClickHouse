-- Regression for STID 2380-3043: `ALTER TABLE <memory_table> (APPLY PATCHES)`
-- (and `APPLY DELETED MASK`) used to trigger
-- `Logical error: Mutation of \`Memory\` table produced incomplete output: block 0 has 0 rows, expected N`
-- because `MutationsInterpreter` skips these commands without producing matching output blocks,
-- and `StorageMemory::mutate` then mismatched the partial-column invariant.
-- `Memory` tables do not own patch parts or deletion masks, so both commands must be no-ops.

DROP TABLE IF EXISTS t_memory_apply_patches;

CREATE TABLE t_memory_apply_patches (id UInt64, value String) ENGINE = Memory;

INSERT INTO t_memory_apply_patches VALUES (1, 'a'), (2, 'b');
INSERT INTO t_memory_apply_patches VALUES (3, 'c'), (4, 'd');

SELECT 'before:', count() FROM t_memory_apply_patches;

-- Parenthesised form (as emitted by BuzzHouse fuzzer)
ALTER TABLE t_memory_apply_patches (APPLY PATCHES);
SELECT 'after parens:', count(), groupArray(id), groupArray(value) FROM (SELECT * FROM t_memory_apply_patches ORDER BY id);

-- Bare form
ALTER TABLE t_memory_apply_patches APPLY PATCHES;
SELECT 'after bare:', count(), groupArray(id), groupArray(value) FROM (SELECT * FROM t_memory_apply_patches ORDER BY id);

-- APPLY DELETED MASK is also a no-op on Memory.
ALTER TABLE t_memory_apply_patches APPLY DELETED MASK;
SELECT 'after deleted mask:', count(), groupArray(id), groupArray(value) FROM (SELECT * FROM t_memory_apply_patches ORDER BY id);

-- Mixed: APPLY PATCHES alongside a real UPDATE must still execute the UPDATE.
ALTER TABLE t_memory_apply_patches UPDATE value = upper(value) WHERE id = 1, APPLY PATCHES SETTINGS mutations_sync = 1;
SELECT 'after mixed:', count(), groupArray(id), groupArray(value) FROM (SELECT * FROM t_memory_apply_patches ORDER BY id);

DROP TABLE t_memory_apply_patches;

-- Memory was the unique outlier: an overridden `mutate` that uses `MutationsInterpreter` and
-- a row-count invariant, paired with an effectively empty `checkMutationIsPossible`. Every
-- other engine either owns patches natively (`MergeTree` family), forwards (`Alias`,
-- `MaterializedView`), or rejects unsupported commands upstream in `checkMutationIsPossible`
-- (`IStorage` default for `Log`/`StripeLog`/`TinyLog`/`Null`/`Set`/...; per-engine override for
-- `Join`/`KeeperMap`/`EmbeddedRocksDB`/`Redis`). Lock that contract in so a future refactor
-- does not silently let `APPLY PATCHES` reach an engine without lightweight-update support.

DROP TABLE IF EXISTS t_log;
CREATE TABLE t_log (id UInt64, value String) ENGINE = Log;
ALTER TABLE t_log APPLY PATCHES; -- { serverError NOT_IMPLEMENTED }
ALTER TABLE t_log APPLY DELETED MASK; -- { serverError NOT_IMPLEMENTED }
DROP TABLE t_log;

DROP TABLE IF EXISTS t_stripelog;
CREATE TABLE t_stripelog (id UInt64, value String) ENGINE = StripeLog;
ALTER TABLE t_stripelog APPLY PATCHES; -- { serverError NOT_IMPLEMENTED }
DROP TABLE t_stripelog;

DROP TABLE IF EXISTS t_tinylog;
CREATE TABLE t_tinylog (id UInt64, value String) ENGINE = TinyLog;
ALTER TABLE t_tinylog APPLY PATCHES; -- { serverError NOT_IMPLEMENTED }
DROP TABLE t_tinylog;

DROP TABLE IF EXISTS t_null;
CREATE TABLE t_null (id UInt64, value String) ENGINE = Null;
ALTER TABLE t_null APPLY PATCHES; -- { serverError NOT_IMPLEMENTED }
DROP TABLE t_null;

DROP TABLE IF EXISTS t_set;
CREATE TABLE t_set (id UInt64, value String) ENGINE = Set;
ALTER TABLE t_set APPLY PATCHES; -- { serverError NOT_IMPLEMENTED }
DROP TABLE t_set;

-- `Join` has its own `checkMutationIsPossible` that throws `NOT_IMPLEMENTED` for non-DELETE.
DROP TABLE IF EXISTS t_join;
CREATE TABLE t_join (id UInt64, value String) ENGINE = Join(ANY, LEFT, id);
ALTER TABLE t_join APPLY PATCHES; -- { serverError NOT_IMPLEMENTED }
DROP TABLE t_join;
