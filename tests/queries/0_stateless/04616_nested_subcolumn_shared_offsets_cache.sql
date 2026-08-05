-- Reading a Nested-element subcolumn (`arr.nested.b`) beside a surviving sibling (`arr.id`) must
-- keep resolving to `name_in_storage = "arr"`, so both share the offsets substreams cache and the
-- shared offsets stream is read only once. After DROP + ADD of `arr.nested` on a Wide part the
-- subcolumn used to resolve to `name_in_storage = "arr.nested"` (the reconstructed Nested type had
-- no `nested` member, because it was referenced only through a subcolumn), splitting the cache. The
-- shared offsets stream was then read twice: with prefetch the sibling's mark seek was suppressed,
-- and across several blocks the second read continued from the wrong position. Either way the
-- sibling `arr.id` came back default-filled.

DROP TABLE IF EXISTS t_shared_wide;
DROP TABLE IF EXISTS t_shared_compact;
DROP TABLE IF EXISTS t_shared_no_offsets;

CREATE TABLE t_shared_wide
(
    id UInt64,
    `arr.id` Array(UInt64),
    `arr.nested` Array(Tuple(a String, b Float64))
)
ENGINE = MergeTree ORDER BY id
SETTINGS share_nested_offsets = 1, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_shared_wide SELECT number, [number + 10], [(toString(number), number * 1.5)] FROM numbers(30000);

ALTER TABLE t_shared_wide DROP COLUMN `arr.nested`;
ALTER TABLE t_shared_wide ADD COLUMN `arr.nested` Array(Tuple(a String, b Float64));

SELECT 'part type', any(part_type) FROM system.parts WHERE database = currentDatabase() AND table = 't_shared_wide' AND active;

-- Content oracles: the surviving sibling keeps its values, and the re-added subcolumn reads one
-- default element per row from the shared offsets. `length` alone is not enough - the bad read
-- corrupted lengths only across blocks, not within one.

-- Single block, prefetch on.
SELECT 'wide prefetch single block',
    sum(length(nb)), sum(arraySum(nb)), countIf(aid != [id + 10])
FROM (SELECT id, `arr.nested`.b AS nb, `arr.id` AS aid FROM t_shared_wide)
SETTINGS local_filesystem_read_prefetch = 1, max_block_size = 100000;

-- Range split across several blocks: exercises the prefetch-independent continue_reading path.
SELECT 'wide multi block',
    sum(length(nb)), sum(arraySum(nb)), countIf(aid != [id + 10])
FROM (SELECT id, `arr.nested`.b AS nb, `arr.id` AS aid FROM t_shared_wide)
SETTINGS local_filesystem_read_prefetch = 1, max_block_size = 1000;

-- Control: Compact parts were never affected.
CREATE TABLE t_shared_compact
(
    id UInt64,
    `arr.id` Array(UInt64),
    `arr.nested` Array(Tuple(a String, b Float64))
)
ENGINE = MergeTree ORDER BY id
SETTINGS share_nested_offsets = 1, min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000;

INSERT INTO t_shared_compact SELECT number, [number + 10], [(toString(number), number * 1.5)] FROM numbers(100);

ALTER TABLE t_shared_compact DROP COLUMN `arr.nested`;
ALTER TABLE t_shared_compact ADD COLUMN `arr.nested` Array(Tuple(a String, b Float64));

SELECT 'compact type', any(part_type) FROM system.parts WHERE database = currentDatabase() AND table = 't_shared_compact' AND active;

SELECT 'compact control',
    sum(length(nb)), sum(arraySum(nb)), countIf(aid != [id + 10])
FROM (SELECT id, `arr.nested`.b AS nb, `arr.id` AS aid FROM t_shared_compact)
SETTINGS local_filesystem_read_prefetch = 1, max_block_size = 1000;

-- Control: without shared offsets `arr.nested` has its own (dropped) offsets stream, so it reads
-- as empty arrays and never contends for the sibling's stream.
CREATE TABLE t_shared_no_offsets
(
    id UInt64,
    `arr.id` Array(UInt64),
    `arr.nested` Array(Tuple(a String, b Float64))
)
ENGINE = MergeTree ORDER BY id
SETTINGS share_nested_offsets = 0, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_shared_no_offsets SELECT number, [number + 10], [(toString(number), number * 1.5)] FROM numbers(100);

ALTER TABLE t_shared_no_offsets DROP COLUMN `arr.nested`;
ALTER TABLE t_shared_no_offsets ADD COLUMN `arr.nested` Array(Tuple(a String, b Float64));

SELECT 'no shared offsets',
    sum(length(nb)), countIf(aid != [id + 10])
FROM (SELECT id, `arr.nested`.b AS nb, `arr.id` AS aid FROM t_shared_no_offsets)
SETTINGS local_filesystem_read_prefetch = 1, max_block_size = 1000;

DROP TABLE t_shared_wide;
DROP TABLE t_shared_compact;
DROP TABLE t_shared_no_offsets;
