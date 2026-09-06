DROP TABLE IF EXISTS t_nested_shared_offsets;

CREATE TABLE t_nested_shared_offsets
(
    id UInt64,
    `arr.id` Array(UInt64),
    `arr.s` Array(String),
    `arr.nested` Array(Tuple(a String, b Float64))
)
ENGINE = MergeTree ORDER BY id
SETTINGS share_nested_offsets = 1, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
         index_granularity = 8192, index_granularity_bytes = 0;

INSERT INTO t_nested_shared_offsets
SELECT number, [number], [toString(number)], [(toString(number), number)] FROM numbers(20000);

ALTER TABLE t_nested_shared_offsets DROP COLUMN `arr.nested`;
ALTER TABLE t_nested_shared_offsets ADD COLUMN `arr.nested` Array(Tuple(a String, b Float64));

-- The re-added member is missing from the part, so its subcolumn is synthesized from the offsets
-- stream `arr.id` and `arr.s` also read. A block boundary inside the range must not corrupt them.
SELECT max_block_size, sum(length(nb)), countIf(aid != [id]), countIf(asx != [toString(id)]), countIf(empty(aid))
FROM
(
    SELECT 100000 AS max_block_size, id, `arr.nested`.b AS nb, `arr.id` AS aid, `arr.s` AS asx
    FROM t_nested_shared_offsets
    SETTINGS max_block_size = 100000, local_filesystem_read_prefetch = 0, max_threads = 1
)
GROUP BY max_block_size;

SELECT max_block_size, sum(length(nb)), countIf(aid != [id]), countIf(asx != [toString(id)]), countIf(empty(aid))
FROM
(
    SELECT 8192 AS max_block_size, id, `arr.nested`.b AS nb, `arr.id` AS aid, `arr.s` AS asx
    FROM t_nested_shared_offsets
    SETTINGS max_block_size = 8192, local_filesystem_read_prefetch = 0, max_threads = 1
)
GROUP BY max_block_size;

SELECT max_block_size, sum(length(nb)), countIf(aid != [id]), countIf(asx != [toString(id)]), countIf(empty(aid))
FROM
(
    SELECT 8000 AS max_block_size, id, `arr.nested`.b AS nb, `arr.id` AS aid, `arr.s` AS asx
    FROM t_nested_shared_offsets
    SETTINGS max_block_size = 8000, local_filesystem_read_prefetch = 0, max_threads = 1
)
GROUP BY max_block_size;

SELECT max_block_size, sum(length(nb)), countIf(aid != [id]), countIf(asx != [toString(id)]), countIf(empty(aid))
FROM
(
    SELECT 1000 AS max_block_size, id, `arr.nested`.b AS nb, `arr.id` AS aid, `arr.s` AS asx
    FROM t_nested_shared_offsets
    SETTINGS max_block_size = 1000, local_filesystem_read_prefetch = 0, max_threads = 1
)
GROUP BY max_block_size;

-- The member's parent co-requested with its subcolumn.
SELECT sum(length(nb)), sum(length(np)), countIf(aid != [id])
FROM
(
    SELECT id, `arr.nested`.b AS nb, `arr.nested` AS np, `arr.id` AS aid
    FROM t_nested_shared_offsets
    SETTINGS max_block_size = 8192, local_filesystem_read_prefetch = 0, max_threads = 1
);

DROP TABLE t_nested_shared_offsets;

DROP TABLE IF EXISTS t_nested_wrapped;

-- Wrapped member value types, read as a depth-1 and a depth-2 subcolumn path.
CREATE TABLE t_nested_wrapped
(
    id UInt64,
    `arr.id` Array(UInt64),
    `arr.n` Array(Nullable(String)),
    `arr.t` Array(Tuple(a String, b Float64)),
    `arr.m` Array(Map(String, UInt64))
)
ENGINE = MergeTree ORDER BY id
SETTINGS share_nested_offsets = 1, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
         index_granularity = 8192, index_granularity_bytes = 0;

INSERT INTO t_nested_wrapped
SELECT number, [number], [toString(number)], [(toString(number), number)], [map(toString(number), number)]
FROM numbers(20000);

ALTER TABLE t_nested_wrapped DROP COLUMN `arr.n`;
ALTER TABLE t_nested_wrapped ADD COLUMN `arr.n` Array(Nullable(String));
ALTER TABLE t_nested_wrapped DROP COLUMN `arr.t`;
ALTER TABLE t_nested_wrapped ADD COLUMN `arr.t` Array(Tuple(a String, b Float64));
ALTER TABLE t_nested_wrapped DROP COLUMN `arr.m`;
ALTER TABLE t_nested_wrapped ADD COLUMN `arr.m` Array(Map(String, UInt64));

SELECT sum(length(`arr.n.null`)), countIf(`arr.id` != [id]) FROM t_nested_wrapped
SETTINGS max_block_size = 8192, local_filesystem_read_prefetch = 0, max_threads = 1;

SELECT sum(length(`arr.t.a`)), countIf(`arr.id` != [id]) FROM t_nested_wrapped
SETTINGS max_block_size = 8192, local_filesystem_read_prefetch = 0, max_threads = 1;

SELECT sum(length(`arr.m.keys`)), countIf(`arr.id` != [id]) FROM t_nested_wrapped
SETTINGS max_block_size = 8192, local_filesystem_read_prefetch = 0, max_threads = 1;

-- A depth-2 path through a Tuple element resolves against the member's own type, not the group's.
SELECT sum(length(`arr.t.a.size`)), countIf(`arr.id` != [id]) FROM t_nested_wrapped
SETTINGS max_block_size = 8192, local_filesystem_read_prefetch = 0, max_threads = 1;

-- `materialize` re-checks the built column against its declared type. A length aggregate alone
-- passes on a wrongly typed column, so every wrapped path is asserted through it as well.
SELECT sum(length(materialize(`arr.m.keys`))), sum(length(materialize(`arr.m.values`))),
       sum(length(materialize(`arr.n.null`))), sum(length(materialize(`arr.t.a`)))
FROM t_nested_wrapped
SETTINGS max_block_size = 8192, local_filesystem_read_prefetch = 0, max_threads = 1;

DROP TABLE t_nested_wrapped;

DROP TABLE IF EXISTS t_nested_map_in_tuple;

-- A Map reached through a Tuple element of the member.
CREATE TABLE t_nested_map_in_tuple
(
    id UInt64,
    `arr.id` Array(UInt64),
    `arr.mt` Array(Tuple(k Map(String, UInt64), z UInt8))
)
ENGINE = MergeTree ORDER BY id
SETTINGS share_nested_offsets = 1, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
         index_granularity = 8192, index_granularity_bytes = 0;

INSERT INTO t_nested_map_in_tuple
SELECT number, [number], [(map(toString(number), number), 1)] FROM numbers(20000);

ALTER TABLE t_nested_map_in_tuple DROP COLUMN `arr.mt`;
ALTER TABLE t_nested_map_in_tuple ADD COLUMN `arr.mt` Array(Tuple(k Map(String, UInt64), z UInt8));

SELECT sum(length(materialize(`arr.mt.k.keys`))), sum(length(materialize(`arr.mt.k`))),
       sum(length(materialize(`arr.mt.z`))), countIf(`arr.id` != [id])
FROM t_nested_map_in_tuple
SETTINGS max_block_size = 8192, local_filesystem_read_prefetch = 0, max_threads = 1;

DROP TABLE t_nested_map_in_tuple;

DROP TABLE IF EXISTS t_nested_wrapped_nullable;

-- A member re-added with a wrapper between the array and the tuple, read through `merge()`, whose
-- converting step rejects a block whose column does not match its declared type.
CREATE TABLE t_nested_wrapped_nullable
(
    id UInt64,
    `arr.id` Array(UInt64),
    `arr.nested` Array(Tuple(a String, b Float64))
)
ENGINE = MergeTree ORDER BY id
SETTINGS share_nested_offsets = 1, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
         index_granularity = 8192, index_granularity_bytes = 0;

INSERT INTO t_nested_wrapped_nullable SELECT number, [number], [(toString(number), number)] FROM numbers(100);

ALTER TABLE t_nested_wrapped_nullable DROP COLUMN `arr.nested` SETTINGS mutations_sync = 2;
ALTER TABLE t_nested_wrapped_nullable ADD COLUMN `arr.nested` Array(Nullable(Tuple(a String, b Float64)))
SETTINGS enable_nullable_tuple_type = 1;

-- The active part no longer stores the member, so its subcolumn is synthesized from the shared offsets.
SELECT arrayStringConcat(arraySort(groupArray(column)), ' ') FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_nested_wrapped_nullable' AND active;

SELECT sum(length(nb)), countIf(aid != [id]) FROM
(
    SELECT id, `arr.nested`.b AS nb, `arr.id` AS aid
    FROM merge(currentDatabase(), '^t_nested_wrapped_nullable$')
);

-- The same read without a converting step, so the block has to be well formed rather than accepted.
-- Every element is the member's own default, which is NULL and not the default of the group's type.
SELECT sum(length(materialize(nb))), sum(arraySum(arrayMap(x -> toUInt8(isNull(x)), nb))), countIf(aid != [id])
FROM (SELECT id, `arr.nested`.b AS nb, `arr.id` AS aid FROM t_nested_wrapped_nullable);

DROP TABLE t_nested_wrapped_nullable;

DROP TABLE IF EXISTS t_nested_wrapped_nullable_compact;

-- The Compact reader discovers a missing member's offsets through its own code path, so the same
-- wrapped member type is asserted on a Compact part as well.
CREATE TABLE t_nested_wrapped_nullable_compact
(
    id UInt64,
    `arr.id` Array(UInt64),
    `arr.nested` Array(Tuple(a String, b Float64))
)
ENGINE = MergeTree ORDER BY id
SETTINGS share_nested_offsets = 1, min_bytes_for_wide_part = 10485760,
         min_rows_for_wide_part = 1000000, index_granularity = 8192,
         index_granularity_bytes = 10485760,
         enable_block_number_column = 0, enable_block_offset_column = 0;

INSERT INTO t_nested_wrapped_nullable_compact
SELECT number, [number], [(toString(number), number)] FROM numbers(100);

ALTER TABLE t_nested_wrapped_nullable_compact DROP COLUMN `arr.nested` SETTINGS mutations_sync = 2;
ALTER TABLE t_nested_wrapped_nullable_compact ADD COLUMN `arr.nested` Array(Nullable(Tuple(a String, b Float64)))
SETTINGS enable_nullable_tuple_type = 1;

SELECT any(part_type) FROM system.parts
WHERE database = currentDatabase() AND table = 't_nested_wrapped_nullable_compact' AND active;

SELECT arrayStringConcat(arraySort(groupArray(column)), ' ') FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_nested_wrapped_nullable_compact' AND active;

SELECT sum(length(materialize(nb))), sum(arraySum(arrayMap(x -> toUInt8(isNull(x)), nb))), countIf(aid != [id])
FROM (SELECT id, `arr.nested`.b AS nb, `arr.id` AS aid FROM t_nested_wrapped_nullable_compact);

DROP TABLE t_nested_wrapped_nullable_compact;
