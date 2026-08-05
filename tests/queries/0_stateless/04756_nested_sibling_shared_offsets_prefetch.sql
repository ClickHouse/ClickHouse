-- Tags: no-random-merge-tree-settings

DROP TABLE IF EXISTS t_shared_offsets_wide;
DROP TABLE IF EXISTS t_shared_offsets_granules;
DROP TABLE IF EXISTS t_shared_offsets_wrapped;
DROP TABLE IF EXISTS t_shared_offsets_compact;

-- A prefetch positions a stream for the column that issued it. Under `share_nested_offsets` several
-- Nested siblings name one offsets stream, so every other column reading it must still seek.
CREATE TABLE t_shared_offsets_wide
(
    `id` UInt64,
    `arr.id` Array(UInt64),
    `arr.s` Array(String),
    `arr.nested` Array(Tuple(a String, b Float64))
)
ENGINE = MergeTree ORDER BY id
SETTINGS share_nested_offsets = 1, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_shared_offsets_wide SELECT
    number,
    arrayMap(x -> x + 10, range(number % 3 + 1)),
    arrayMap(x -> concat('s', toString(x)), range(number % 3 + 1)),
    arrayMap(x -> (toString(x), x * 1.5), range(number % 3 + 1))
FROM numbers(100);

ALTER TABLE t_shared_offsets_wide DROP COLUMN `arr.nested`;
ALTER TABLE t_shared_offsets_wide ADD COLUMN `arr.nested` Array(Tuple(a String, b Float64));

SELECT 'part type', any(part_type) FROM system.parts WHERE database = currentDatabase() AND table = 't_shared_offsets_wide' AND active;

-- The reader must be asked for both columns, otherwise `query_plan_remove_unused_columns` prunes the
-- subcolumn and the shared stream is read by a single column.
SELECT 'both columns in reader header', countIf(explain LIKE '%arr.nested.b%') > 0 AND countIf(explain LIKE '%arr.id%') > 0
FROM (EXPLAIN header = 1
    SELECT sum(length(nb)), countIf(aid != arrayMap(x -> x + 10, range(id % 3 + 1)))
    FROM (SELECT id, `arr.nested`.b AS nb, `arr.id` AS aid FROM t_shared_offsets_wide));

SET local_filesystem_read_prefetch = 1;

-- A nonzero `filesystem_prefetches_limit` below the number of columns read skips prefetching
-- entirely, which would make every assertion below pass without taking the prefetch path. Read the
-- effective value rather than pinning it, so such a limit fails this test instead of silencing it.
-- No query below reads more than 8 columns; 0 means unlimited.
SELECT 'prefetch limit permits prefetching', getSetting('filesystem_prefetches_limit') = 0 OR getSetting('filesystem_prefetches_limit') > 8;

SELECT 'missing subcolumn first', sum(length(nb)), countIf(aid != arrayMap(x -> x + 10, range(id % 3 + 1))), countIf(s != arrayMap(x -> concat('s', toString(x)), range(id % 3 + 1)))
FROM (SELECT id, `arr.nested`.b AS nb, `arr.id` AS aid, `arr.s` AS s FROM t_shared_offsets_wide);

SELECT 'sibling first', sum(length(nb)), countIf(aid != arrayMap(x -> x + 10, range(id % 3 + 1))), countIf(s != arrayMap(x -> concat('s', toString(x)), range(id % 3 + 1)))
FROM (SELECT id, `arr.id` AS aid, `arr.s` AS s, `arr.nested`.b AS nb FROM t_shared_offsets_wide);

SELECT 'sibling between', sum(length(nb)), countIf(aid != arrayMap(x -> x + 10, range(id % 3 + 1))), countIf(s != arrayMap(x -> concat('s', toString(x)), range(id % 3 + 1)))
FROM (SELECT id, `arr.id` AS aid, `arr.nested`.b AS nb, `arr.s` AS s FROM t_shared_offsets_wide);

SELECT 'prefetch off', sum(length(nb)), countIf(aid != arrayMap(x -> x + 10, range(id % 3 + 1)))
FROM (SELECT id, `arr.nested`.b AS nb, `arr.id` AS aid FROM t_shared_offsets_wide)
SETTINGS local_filesystem_read_prefetch = 0;

-- More than one granule, so the seek must be right for every mark, not only the first.
CREATE TABLE t_shared_offsets_granules
(
    `id` UInt64,
    `arr.id` Array(UInt64),
    `arr.nested` Array(Tuple(a String, b Float64))
)
ENGINE = MergeTree ORDER BY id
SETTINGS share_nested_offsets = 1, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 8;

INSERT INTO t_shared_offsets_granules SELECT
    number,
    arrayMap(x -> x + 10, range(number % 3 + 1)),
    arrayMap(x -> (toString(x), x * 1.5), range(number % 3 + 1))
FROM numbers(100);

ALTER TABLE t_shared_offsets_granules DROP COLUMN `arr.nested`;
ALTER TABLE t_shared_offsets_granules ADD COLUMN `arr.nested` Array(Tuple(a String, b Float64));

SELECT 'many granules', sum(length(nb)), countIf(aid != arrayMap(x -> x + 10, range(id % 3 + 1)))
FROM (SELECT id, `arr.nested`.b AS nb, `arr.id` AS aid FROM t_shared_offsets_granules);

-- Wrapped element types resolve to the same shared offsets stream.
CREATE TABLE t_shared_offsets_wrapped
(
    `id` UInt64,
    `arr.n` Array(Nullable(UInt64)),
    `arr.lc` Array(LowCardinality(String)),
    `arr.nested` Array(Tuple(a String, b Float64))
)
ENGINE = MergeTree ORDER BY id
SETTINGS share_nested_offsets = 1, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_shared_offsets_wrapped SELECT
    number,
    arrayMap(x -> if(x % 2 = 0, NULL, x + 10), range(number % 3 + 1)),
    arrayMap(x -> concat('l', toString(x)), range(number % 3 + 1)),
    arrayMap(x -> (toString(x), x * 1.5), range(number % 3 + 1))
FROM numbers(100);

ALTER TABLE t_shared_offsets_wrapped DROP COLUMN `arr.nested`;
ALTER TABLE t_shared_offsets_wrapped ADD COLUMN `arr.nested` Array(Tuple(a String, b Float64));

SELECT 'wrapped element types', sum(length(nb)), countIf(n != arrayMap(x -> if(x % 2 = 0, NULL, x + 10), range(id % 3 + 1))), countIf(lc != arrayMap(x -> concat('l', toString(x)), range(id % 3 + 1)))
FROM (SELECT id, `arr.nested`.b AS nb, `arr.n` AS n, `arr.lc` AS lc FROM t_shared_offsets_wrapped);

-- Compact parts keep per-column offsets and were never affected; a control against overfitting.
CREATE TABLE t_shared_offsets_compact
(
    `id` UInt64,
    `arr.id` Array(UInt64),
    `arr.nested` Array(Tuple(a String, b Float64))
)
ENGINE = MergeTree ORDER BY id
SETTINGS share_nested_offsets = 1, min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000;

INSERT INTO t_shared_offsets_compact SELECT
    number,
    arrayMap(x -> x + 10, range(number % 3 + 1)),
    arrayMap(x -> (toString(x), x * 1.5), range(number % 3 + 1))
FROM numbers(100);

ALTER TABLE t_shared_offsets_compact DROP COLUMN `arr.nested`;
ALTER TABLE t_shared_offsets_compact ADD COLUMN `arr.nested` Array(Tuple(a String, b Float64));

SELECT 'compact part type', any(part_type) FROM system.parts WHERE database = currentDatabase() AND table = 't_shared_offsets_compact' AND active;

SELECT 'compact control', sum(length(nb)), countIf(aid != arrayMap(x -> x + 10, range(id % 3 + 1)))
FROM (SELECT id, `arr.nested`.b AS nb, `arr.id` AS aid FROM t_shared_offsets_compact);

DROP TABLE t_shared_offsets_wide;
DROP TABLE t_shared_offsets_granules;
DROP TABLE t_shared_offsets_wrapped;
DROP TABLE t_shared_offsets_compact;
