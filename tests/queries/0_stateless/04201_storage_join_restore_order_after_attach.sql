-- Test: exercises `StorageSetOrJoinBase::restore` priority_queue file ordering for `StorageJoin`
-- Covers: src/Storages/StorageSet.cpp:286,308-312 — restore loads .bin files in ascending file_num order
-- so that ANY-strictness Join keeps the FIRST inserted row per key after table reload (DETACH+ATTACH).

DROP TABLE IF EXISTS test_join_restore_order;

-- ANY without join_any_take_last_row: first inserted row per key wins.
-- Each separate INSERT creates a new .bin file (1.bin, 2.bin, 3.bin, ...).
-- Before the fix, dir_it iteration order was filesystem-dependent so reload
-- could produce a non-deterministic winner. After the fix the priority_queue
-- restores ascending by file_num so the in-memory state is identical to the
-- state right after inserts.
CREATE TABLE test_join_restore_order (k UInt32, v String) ENGINE = Join(ANY, LEFT, k);

INSERT INTO test_join_restore_order VALUES (1, 'first');
INSERT INTO test_join_restore_order VALUES (1, 'second');
INSERT INTO test_join_restore_order VALUES (1, 'third');
INSERT INTO test_join_restore_order VALUES (2, 'a_first');
INSERT INTO test_join_restore_order VALUES (2, 'b_second');

SELECT 'before-detach', joinGet('test_join_restore_order', 'v', toUInt32(1));
SELECT 'before-detach', joinGet('test_join_restore_order', 'v', toUInt32(2));

DETACH TABLE test_join_restore_order;
ATTACH TABLE test_join_restore_order;

SELECT 'after-attach', joinGet('test_join_restore_order', 'v', toUInt32(1));
SELECT 'after-attach', joinGet('test_join_restore_order', 'v', toUInt32(2));

DROP TABLE test_join_restore_order;

-- With join_any_take_last_row=1: last inserted row per key wins.
-- The same restore-ordering fix must keep the in-memory result the same
-- (last file processed = highest file_num = last INSERT) after reload.
CREATE TABLE test_join_restore_order (k UInt32, v String) ENGINE = Join(ANY, LEFT, k) SETTINGS join_any_take_last_row = 1;

INSERT INTO test_join_restore_order VALUES (1, 'first');
INSERT INTO test_join_restore_order VALUES (1, 'second');
INSERT INTO test_join_restore_order VALUES (1, 'third');

SELECT 'last-row-before', joinGet('test_join_restore_order', 'v', toUInt32(1));

DETACH TABLE test_join_restore_order;
ATTACH TABLE test_join_restore_order;

SELECT 'last-row-after', joinGet('test_join_restore_order', 'v', toUInt32(1));

DROP TABLE test_join_restore_order;
