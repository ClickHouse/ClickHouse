-- Tests ALTER TABLE ... RECOMPRESS COLUMN on Nested siblings that share their offsets stream
-- (`share_nested_offsets = 1`): `n.a` and `n.b` are stored with a single shared `n.size0` stream.
-- The wide fast path recompresses one column at a time, so the shared offsets stream is reached by
-- every sibling; it must be rewritten exactly once (by the first sibling), otherwise the shared
-- `.bin`/marks files would be written several times and the codec of the last writer would silently
-- win. Whichever way, the data and the marks must stay consistent -- CHECK TABLE recomputes them.

SET mutations_sync = 2;
SET check_query_single_value_result = 1;

-- Two siblings with *different* codecs, sharing the offsets stream, on wide parts.
DROP TABLE IF EXISTS t_recompress_shared;

CREATE TABLE t_recompress_shared
(
    id UInt64,
    `n.a` Array(UInt64) CODEC(NONE),
    `n.b` Array(String) CODEC(NONE)
)
ENGINE = MergeTree ORDER BY id
SETTINGS share_nested_offsets = 1, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

-- Matching array sizes are required for shared offsets.
INSERT INTO t_recompress_shared
SELECT number, range(number % 5), arrayMap(x -> toString(x), range(number % 5)) FROM numbers(100000);

SELECT DISTINCT 'shared part', part_type FROM system.parts
WHERE database = currentDatabase() AND table = 't_recompress_shared' AND active;

SELECT 'shared before', count(), sum(length(n.a)), sum(length(n.b)),
    countIf(n.a = arrayMap(x -> x, range(id % 5))), countIf(n.b = arrayMap(x -> toString(x), range(id % 5)))
FROM t_recompress_shared;

-- Give the two siblings different generic codecs, then recompress both in one ALTER. The shared
-- offsets stream is generic-codec-only, so the two passes would pick different codecs for it.
ALTER TABLE t_recompress_shared MODIFY COLUMN `n.a` Array(UInt64) CODEC(ZSTD);
ALTER TABLE t_recompress_shared MODIFY COLUMN `n.b` Array(String) CODEC(LZ4);
ALTER TABLE t_recompress_shared RECOMPRESS COLUMN `n.a`, RECOMPRESS COLUMN `n.b`;

SELECT 'shared after', count(), sum(length(n.a)), sum(length(n.b)),
    countIf(n.a = arrayMap(x -> x, range(id % 5))), countIf(n.b = arrayMap(x -> toString(x), range(id % 5)))
FROM t_recompress_shared;

-- Point lookup and scattered scan exercise the rewritten marks of the shared offsets stream.
SELECT 'shared point', n.a, n.b FROM t_recompress_shared WHERE id = 99999;
SELECT 'shared scan', count() FROM t_recompress_shared WHERE id % 7 = 0 AND length(n.a) = length(n.b);

CHECK TABLE t_recompress_shared;

DROP TABLE t_recompress_shared;

-- Recompress only ONE sibling: the shared offsets stream is rewritten with that sibling's codec, and
-- the other (untouched) sibling must still read its arrays correctly through the same shared stream.
DROP TABLE IF EXISTS t_recompress_shared_one;

CREATE TABLE t_recompress_shared_one
(
    id UInt64,
    `n.a` Array(UInt64) CODEC(NONE),
    `n.b` Array(String) CODEC(NONE)
)
ENGINE = MergeTree ORDER BY id
SETTINGS share_nested_offsets = 1, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_recompress_shared_one
SELECT number, range(number % 5), arrayMap(x -> toString(x), range(number % 5)) FROM numbers(100000);

ALTER TABLE t_recompress_shared_one MODIFY COLUMN `n.a` Array(UInt64) CODEC(ZSTD);
ALTER TABLE t_recompress_shared_one RECOMPRESS COLUMN `n.a`;

SELECT 'one sibling after', count(), sum(length(n.a)), sum(length(n.b)),
    countIf(n.a = arrayMap(x -> x, range(id % 5))), countIf(n.b = arrayMap(x -> toString(x), range(id % 5)))
FROM t_recompress_shared_one;

CHECK TABLE t_recompress_shared_one;

DROP TABLE t_recompress_shared_one;
