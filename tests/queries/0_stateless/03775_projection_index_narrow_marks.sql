-- Tags: no-random-settings, no-random-merge-tree-settings
-- Exercises projection_index_narrow_marks across a mixed-match multi-part
-- dataset. The cases that matter:
--   - parts with no matching rows (narrow path should prune them entirely
--     instead of distributing every mark to reader threads),
--   - multiple matches within a single granule (coalesce to one MarkRange),
--   - multiple matching granules in one part (per-part MarkRanges with > 1 range),
--   - a predicate the projection PK handles but that matches nothing
--     (drives the empty-batch `continue` in MergeTreeReadPool::getTask).

SET enable_analyzer = 1;
SET min_table_rows_to_use_projection_index = 0;
SET max_projection_rows_to_use_projection_index = 1000000000;

DROP TABLE IF EXISTS t_narrow;

CREATE TABLE t_narrow
(
    id UInt64,
    trace_id FixedString(16),
    payload String,
    PROJECTION by_trace_id (SELECT _part_offset ORDER BY trace_id)
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 16;

-- Five parts of 5000 rows each. Parts 1 and 3 have NO matches for the needle,
-- so the narrow path must prune them entirely. Part 2 has three matching rows
-- spanning two granules (50, 51 in one granule; 100 in another).
INSERT INTO t_narrow SELECT number, if(number = 100, unhex('00000000000000000000000000000042'), unhex(hex(sipHash128(toString(number))))), 'x' FROM numbers(5000);
INSERT INTO t_narrow SELECT 5000 + number, unhex(hex(sipHash128(toString(5000 + number)))), 'x' FROM numbers(5000);
INSERT INTO t_narrow SELECT 10000 + number, if(number IN (50, 51, 100), unhex('00000000000000000000000000000042'), unhex(hex(sipHash128(toString(10000 + number))))), 'x' FROM numbers(5000);
INSERT INTO t_narrow SELECT 15000 + number, unhex(hex(sipHash128(toString(15000 + number)))), 'x' FROM numbers(5000);
INSERT INTO t_narrow SELECT 20000 + number, if(number = 10, unhex('00000000000000000000000000000042'), unhex(hex(sipHash128(toString(20000 + number))))), 'x' FROM numbers(5000);

-- 1. On and off return the exact same ids. Explicit listing catches any row
--    drop or duplication.
SELECT 'off', id
FROM t_narrow WHERE trace_id = unhex('00000000000000000000000000000042')
ORDER BY id
SETTINGS projection_index_narrow_marks = 0;

SELECT 'on', id
FROM t_narrow WHERE trace_id = unhex('00000000000000000000000000000042')
ORDER BY id
SETTINGS projection_index_narrow_marks = 1;

-- 2. Symmetric difference: zero rows if the two sets agree. EXCEPT is used
--    twice so that a spurious row in either direction fails the check.
SELECT 'diff_off_only', count()
FROM (
    SELECT id FROM t_narrow WHERE trace_id = unhex('00000000000000000000000000000042') SETTINGS projection_index_narrow_marks = 0
    EXCEPT
    SELECT id FROM t_narrow WHERE trace_id = unhex('00000000000000000000000000000042') SETTINGS projection_index_narrow_marks = 1
);

SELECT 'diff_on_only', count()
FROM (
    SELECT id FROM t_narrow WHERE trace_id = unhex('00000000000000000000000000000042') SETTINGS projection_index_narrow_marks = 1
    EXCEPT
    SELECT id FROM t_narrow WHERE trace_id = unhex('00000000000000000000000000000042') SETTINGS projection_index_narrow_marks = 0
);

-- 3. Missing key: every part gets pruned.
SELECT 'missing', count()
FROM t_narrow WHERE trace_id = unhex('deadbeefdeadbeefdeadbeefdeadbeef')
SETTINGS projection_index_narrow_marks = 1;

-- 4. Predicate the projection PK handles but that matches no rows: every
--    batch narrows to empty, drives the `continue` in getTask.
SELECT 'empty_range', count()
FROM t_narrow WHERE trace_id > unhex('fffffffffffffffffffffffffffffffe')
SETTINGS projection_index_narrow_marks = 1;

-- 5. Read-in-order query shape (ORDER BY matches the table's sort key). Goes
--    through MergeTreeReadPoolInOrder rather than the default pool.
SELECT 'in_order_limit', id
FROM t_narrow WHERE trace_id = unhex('00000000000000000000000000000042')
ORDER BY id LIMIT 3
SETTINGS projection_index_narrow_marks = 1, optimize_read_in_order = 1;

-- 6. Concurrent readers. The batch-selection split in pickNextBatch + the
--    narrow / continue loop in getTask have to stay correct when multiple
--    threads are pulling batches and stealing from each other.
SELECT 'parallel', id
FROM t_narrow WHERE trace_id = unhex('00000000000000000000000000000042')
ORDER BY id
SETTINGS projection_index_narrow_marks = 1, max_threads = 4;

DROP TABLE t_narrow;

-- 7. Adaptive granularity: same shape as (1), but the table uses
--    index_granularity_bytes so MergeTreeIndexGranularityAdaptive is selected
--    and bitmapToMarkRanges exercises its monotonic-cursor path rather than
--    the constant fast path.
DROP TABLE IF EXISTS t_narrow_adaptive;
CREATE TABLE t_narrow_adaptive
(
    id UInt64,
    trace_id FixedString(16),
    payload String,
    PROJECTION by_trace_id (SELECT _part_offset ORDER BY trace_id)
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 128, index_granularity_bytes = 1024;

INSERT INTO t_narrow_adaptive SELECT number, if(number = 100, unhex('00000000000000000000000000000042'), unhex(hex(sipHash128(toString(number))))), 'x' FROM numbers(5000);
INSERT INTO t_narrow_adaptive SELECT 5000 + number, unhex(hex(sipHash128(toString(5000 + number)))), 'x' FROM numbers(5000);
INSERT INTO t_narrow_adaptive SELECT 10000 + number, if(number IN (50, 51, 100), unhex('00000000000000000000000000000042'), unhex(hex(sipHash128(toString(10000 + number))))), 'x' FROM numbers(5000);
INSERT INTO t_narrow_adaptive SELECT 15000 + number, unhex(hex(sipHash128(toString(15000 + number)))), 'x' FROM numbers(5000);
INSERT INTO t_narrow_adaptive SELECT 20000 + number, if(number = 10, unhex('00000000000000000000000000000042'), unhex(hex(sipHash128(toString(20000 + number))))), 'x' FROM numbers(5000);

SELECT 'adaptive_off', id
FROM t_narrow_adaptive WHERE trace_id = unhex('00000000000000000000000000000042')
ORDER BY id
SETTINGS projection_index_narrow_marks = 0;

SELECT 'adaptive_on', id
FROM t_narrow_adaptive WHERE trace_id = unhex('00000000000000000000000000000042')
ORDER BY id
SETTINGS projection_index_narrow_marks = 1;

DROP TABLE t_narrow_adaptive;
