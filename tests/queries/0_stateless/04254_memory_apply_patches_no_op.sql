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
