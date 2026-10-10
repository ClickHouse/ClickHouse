DROP TABLE IF EXISTS test_flatten_nested_crash;
CREATE TABLE test_flatten_nested_crash
(
    `id` UInt64,
    `tenant` String,
    `arr.id` Array(Nullable(UInt64)),
    `arr.name` Array(Nullable(String)),
    `arr.nested` Array(Tuple(a String, b Float64))
)
ENGINE = MergeTree
ORDER BY (id)
SETTINGS index_granularity = 8192;
INSERT INTO test_flatten_nested_crash
SELECT * FROM generateRandom(
    '`id` UInt64,
    `tenant` String,
    `arr.id` Array(Nullable(UInt64)),
    `arr.name` Array(Nullable(String)),
    `arr.nested` Array(Tuple(a String, b Float64))', 1, 10
) LIMIT 1;
ALTER TABLE test_flatten_nested_crash DROP COLUMN `arr.nested`;
ALTER TABLE test_flatten_nested_crash ADD COLUMN `arr.nested` Array(Tuple(a String, b Float64));
SELECT arr.nested FROM test_flatten_nested_crash ORDER BY arr.nested LIMIT 1;
SELECT 'subcolumn and parent', arr.nested.b, arr.nested FROM test_flatten_nested_crash ORDER BY arr.nested LIMIT 1;
DROP TABLE test_flatten_nested_crash;

-- Reading a re-added Nested element subcolumn together with its own parent column used to put the
-- whole Tuple in the subcolumn's slot while the block kept declaring the element type: a logical
-- error in debug builds, and a wrongly-typed read of unrelated memory in release builds.
-- share_nested_offsets (default on) is load-bearing, so pin it instead of disabling randomization.

DROP TABLE IF EXISTS test_nested_subcolumn_refill;
CREATE TABLE test_nested_subcolumn_refill
(
    `id` UInt64,
    `arr.id` Array(UInt64),
    `arr.nested` Array(Tuple(a String, b Float64))
)
ENGINE = MergeTree ORDER BY (id) SETTINGS share_nested_offsets = 1;
INSERT INTO test_nested_subcolumn_refill VALUES (1, [1], [('y', 2.5)]);
ALTER TABLE test_nested_subcolumn_refill DROP COLUMN `arr.nested`;
ALTER TABLE test_nested_subcolumn_refill ADD COLUMN `arr.nested` Array(Tuple(a String, b Float64));

-- The subcolumn must read the default, consistently with the parent.
SELECT 'float element', arr.nested.b, arr.nested FROM test_nested_subcolumn_refill;
SELECT 'string element', arr.nested.a, arr.nested FROM test_nested_subcolumn_refill;
-- ORDER BY hands the same block to the merging consumer.
SELECT 'ordered by parent', arr.nested.b FROM test_nested_subcolumn_refill ORDER BY arr.nested;
-- Function execution over the subcolumn is a third consumer of the same block.
SELECT 'array element', arr.nested.b[1], arr.nested FROM test_nested_subcolumn_refill;
SELECT 'array sum', arraySum(arr.nested.b), arr.nested FROM test_nested_subcolumn_refill;

-- Reads that were already correct and must stay so.
SELECT 'subcolumn alone', arr.nested.b FROM test_nested_subcolumn_refill;
SELECT 'parent alone', arr.nested FROM test_nested_subcolumn_refill;
SELECT 'two subcolumns', arr.nested.b, arr.nested.a FROM test_nested_subcolumn_refill;
SELECT 'size0 and parent', arr.nested.size0, arr.nested FROM test_nested_subcolumn_refill;

-- One part missing the column, one part having it.
INSERT INTO test_nested_subcolumn_refill VALUES (2, [2], [('z', 7.5)]);
SELECT 'two parts', id, arr.nested.b, arr.nested FROM test_nested_subcolumn_refill ORDER BY id;
DROP TABLE test_nested_subcolumn_refill;

-- Wide and Compact parts reach the same code, so pin min_bytes_for_wide_part to cover each.
DROP TABLE IF EXISTS test_nested_subcolumn_refill_wide;
CREATE TABLE test_nested_subcolumn_refill_wide
(
    `id` UInt64,
    `arr.id` Array(UInt64),
    `arr.nested` Array(Tuple(a String, b Float64))
)
ENGINE = MergeTree ORDER BY (id) SETTINGS min_bytes_for_wide_part = 0, share_nested_offsets = 1;
INSERT INTO test_nested_subcolumn_refill_wide VALUES (1, [1], [('y', 2.5)]);
ALTER TABLE test_nested_subcolumn_refill_wide DROP COLUMN `arr.nested`;
ALTER TABLE test_nested_subcolumn_refill_wide ADD COLUMN `arr.nested` Array(Tuple(a String, b Float64));
SELECT 'wide part', part_type FROM system.parts WHERE database = currentDatabase() AND table = 'test_nested_subcolumn_refill_wide' AND active;
SELECT 'wide read', arr.nested.b, arr.nested FROM test_nested_subcolumn_refill_wide;
DROP TABLE test_nested_subcolumn_refill_wide;

DROP TABLE IF EXISTS test_nested_subcolumn_refill_compact;
CREATE TABLE test_nested_subcolumn_refill_compact
(
    `id` UInt64,
    `arr.id` Array(UInt64),
    `arr.nested` Array(Tuple(a String, b Float64))
)
ENGINE = MergeTree ORDER BY (id) SETTINGS min_bytes_for_wide_part = 1000000000, share_nested_offsets = 1;
INSERT INTO test_nested_subcolumn_refill_compact VALUES (1, [1], [('y', 2.5)]);
ALTER TABLE test_nested_subcolumn_refill_compact DROP COLUMN `arr.nested`;
ALTER TABLE test_nested_subcolumn_refill_compact ADD COLUMN `arr.nested` Array(Tuple(a String, b Float64));
SELECT 'compact part', part_type FROM system.parts WHERE database = currentDatabase() AND table = 'test_nested_subcolumn_refill_compact' AND active;
SELECT 'compact read', arr.nested.b, arr.nested FROM test_nested_subcolumn_refill_compact;
DROP TABLE test_nested_subcolumn_refill_compact;

-- Subcolumns served from the shared offsets rather than derived from the parent were already
-- correct and must not change.
DROP TABLE IF EXISTS test_nested_subcolumn_refill_null;
CREATE TABLE test_nested_subcolumn_refill_null
(
    `id` UInt64,
    `arr.id` Array(UInt64),
    `arr.nested` Array(Nullable(Float64))
)
ENGINE = MergeTree ORDER BY (id) SETTINGS share_nested_offsets = 1;
INSERT INTO test_nested_subcolumn_refill_null VALUES (1, [1], [2.5]);
ALTER TABLE test_nested_subcolumn_refill_null DROP COLUMN `arr.nested`;
ALTER TABLE test_nested_subcolumn_refill_null ADD COLUMN `arr.nested` Array(Nullable(Float64));
SELECT 'null subcolumn', arr.nested.null, arr.nested FROM test_nested_subcolumn_refill_null;
DROP TABLE test_nested_subcolumn_refill_null;

-- Same shape with a DDL DEFAULT on the re-added column, read on the old part. The DDL default
-- must win here, and reading the element beside its parent hit the same wrongly-typed slot.
DROP TABLE IF EXISTS test_nested_subcolumn_refill_default;
CREATE TABLE test_nested_subcolumn_refill_default
(
    `id` UInt64,
    `arr.id` Array(UInt64),
    `arr.nested` Array(Tuple(a String, b Float64))
)
ENGINE = MergeTree ORDER BY (id) SETTINGS share_nested_offsets = 1;
INSERT INTO test_nested_subcolumn_refill_default VALUES (1, [1], [('y', 2.5)]);
ALTER TABLE test_nested_subcolumn_refill_default DROP COLUMN `arr.nested`;
ALTER TABLE test_nested_subcolumn_refill_default ADD COLUMN `arr.nested` Array(Tuple(a String, b Float64)) DEFAULT [('dflt', 42.5)];
SELECT 'default subcolumn alone', arr.nested.b FROM test_nested_subcolumn_refill_default;
SELECT 'default subcolumn and parent', arr.nested.b, arr.nested FROM test_nested_subcolumn_refill_default;
SELECT 'default parent alone', arr.nested FROM test_nested_subcolumn_refill_default;
SELECT 'default string element', arr.nested.a, arr.nested FROM test_nested_subcolumn_refill_default;
DROP TABLE test_nested_subcolumn_refill_default;

-- A DEFAULT that reads a sibling column proves the expression is evaluated rather than the
-- element being synthesized from the shared offsets, which would yield the type default.
DROP TABLE IF EXISTS test_nested_subcolumn_refill_default_sibling;
CREATE TABLE test_nested_subcolumn_refill_default_sibling
(
    `id` UInt64,
    `arr.id` Array(UInt64),
    `arr.nested` Array(Tuple(a String, b Float64))
)
ENGINE = MergeTree ORDER BY (id) SETTINGS share_nested_offsets = 1;
INSERT INTO test_nested_subcolumn_refill_default_sibling VALUES (7, [1], [('y', 2.5)]);
ALTER TABLE test_nested_subcolumn_refill_default_sibling DROP COLUMN `arr.nested`;
ALTER TABLE test_nested_subcolumn_refill_default_sibling ADD COLUMN `arr.nested` Array(Tuple(a String, b Float64)) DEFAULT arrayMap(x -> ('sib', toFloat64(id)), `arr.id`);
SELECT 'sibling default subcolumn', arr.nested.b, arr.nested FROM test_nested_subcolumn_refill_default_sibling;
DROP TABLE test_nested_subcolumn_refill_default_sibling;
