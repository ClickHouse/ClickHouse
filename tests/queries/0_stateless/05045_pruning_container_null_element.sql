-- Range-based pruning must not drop rows when a container value has a NULL element: the range
-- arithmetic orders a nested Null before every value, while the data and the predicate order it after
-- every value, so a range over such values does not bound what the predicate compares.
-- The keep-pruning arms are EXPLAIN arms on purpose. Declining an atom only ever over-reads, so it
-- cannot change an answer, and only a granule count can show that pruning was kept.

SET enable_nullable_tuple_type = 1;

-- minmax skip index over Array(Nullable(String))
CREATE TABLE t_arr (id UInt64, a Array(Nullable(String)), INDEX i a TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 3, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_arr VALUES (1, ['a']), (2, [NULL]), (3, ['c']);

SELECT 'arr_gt', count() FROM t_arr WHERE a > ['zzz'] SETTINGS use_skip_indexes = 1;

-- minmax skip index over Map(String, Nullable(String))
CREATE TABLE t_map (id UInt64, a Map(String, Nullable(String)), INDEX i a TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 3, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_map VALUES (1, map('k', 'a')), (2, map('k', NULL)), (3, map('k', 'c'));

SELECT 'map_gt', count() FROM t_map WHERE a > map('k', 'zzz') SETTINGS use_skip_indexes = 1;

-- part-level minmax index over a partition key expression
CREATE TABLE t_part (id UInt64, a Array(Nullable(String)))
ENGINE = MergeTree PARTITION BY length(a) ORDER BY id
SETTINGS index_granularity = 3, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_part VALUES (1, ['a']), (2, [NULL]), (3, ['c']);

SELECT 'part_gt', count() FROM t_part WHERE a > ['zzz'];

-- partition pruner, and the trivial count by a partition predicate
CREATE TABLE t_partval (id UInt64, a Array(Nullable(String)))
ENGINE = MergeTree PARTITION BY a ORDER BY id
SETTINGS index_granularity = 3, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, allow_nullable_key = 1;
INSERT INTO t_partval VALUES (1, ['a']), (2, [NULL]), (3, ['c']);

SELECT 'partval_gt', arraySort(groupArray(id)) FROM t_partval WHERE a > ['b'];
SELECT 'partval_count', count() FROM t_partval WHERE a > ['b'] SETTINGS optimize_trivial_count_query = 1;

-- set skip index: its hyperrectangle is consulted only when bulk filtering is off
CREATE TABLE t_set (id UInt64, a Array(Nullable(String)), INDEX i a TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 3, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_set VALUES (1, ['a']), (2, ['b']), (3, ['c']), (4, ['x']), (5, [NULL]), (6, ['z']);

SELECT 'set_gt', count() FROM t_set WHERE a > ['zzz'] SETTINGS use_skip_indexes = 1, secondary_indices_enable_bulk_filtering = 0;

-- primary key over Array(Nullable(String)): the row that disappears is an ordinary one
CREATE TABLE t_pk_arr (id UInt64, a Array(Nullable(String)))
ENGINE = MergeTree ORDER BY a
SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, allow_nullable_key = 1;
INSERT INTO t_pk_arr VALUES (1, ['a']), (2, [NULL]), (3, ['c']);

SELECT 'pk_arr', arraySort(groupArray(id)) FROM t_pk_arr WHERE a > ['b'];

-- primary key over Tuple(Nullable(String)): a Tuple establishes container context
CREATE TABLE t_pk_tup (id UInt64, a Tuple(Nullable(String)))
ENGINE = MergeTree ORDER BY a
SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, allow_nullable_key = 1;
INSERT INTO t_pk_tup VALUES (1, tuple('a')), (2, tuple(NULL)), (3, tuple('c'));

SELECT 'pk_tup', arraySort(groupArray(id)) FROM t_pk_tup WHERE a > tuple('b');

-- primary key over Nullable(Tuple(Nullable(String))): a top-level Nullable must not end the walk
CREATE TABLE t_pk_ntup (id UInt64, a Nullable(Tuple(Nullable(String))))
ENGINE = MergeTree ORDER BY a
SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, allow_nullable_key = 1;
INSERT INTO t_pk_ntup VALUES (1, tuple('a')), (2, tuple(NULL)), (3, tuple('c'));

SELECT 'pk_ntup', arraySort(groupArray(id)) FROM t_pk_ntup WHERE a > tuple('b');

-- an atom can read several key columns at once, here `(u, a) IN (...)` over a composite index
CREATE TABLE t_comp_in (id UInt64, u UInt64, a Array(Nullable(String)), INDEX i (u, a) TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 3, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_comp_in VALUES (1, 2, ['a']), (2, 2, [NULL]), (3, 2, ['c']), (4, 2, ['x']), (5, 2, ['y']), (6, 2, ['z']);

SELECT 'comp_in', count() FROM t_comp_in WHERE (u, a) IN ((2, [NULL])) SETTINGS use_skip_indexes = 1;

-- A projection sorting key reaches the same arithmetic, and two properties are unique to this arm:
-- `json.c[].d.:Int64` is `Array(Nullable(Int64))` though no column declares that type, and a projection
-- sorting key needs no `allow_nullable_key`. The element is NULL wherever the JSON path is absent.
CREATE TABLE t_json_proj (id UInt32, json JSON, PROJECTION p (SELECT json, id ORDER BY json.c[].d.:Int64))
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;
INSERT INTO t_json_proj SELECT number, toJSONString(map('c', [toJSONString(map('d', number::UInt32))::JSON]))
FROM numbers(1, 8) SETTINGS use_variant_as_common_type = 1;
INSERT INTO t_json_proj VALUES (9, '{"c":[{"e":1}]}');

SELECT 'json_proj', arraySort(groupArray(id)) FROM t_json_proj WHERE json.c[].d.:Int64 > [6] SETTINGS optimize_use_projections = 1;

-- Keep-pruning arms.

-- Array(Float64) with a NaN: the two orders agree for a nested NaN, so both the index and the
-- primary key must keep pruning
CREATE TABLE t_keep_float (id UInt64, a Array(Float64), INDEX i a TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY a
SETTINGS index_granularity = 3, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_keep_float VALUES (1, [1.]), (2, [2.]), (3, [3.]), (4, [4.]), (5, [20.]), (6, [nan]);

SELECT 'keep_float_pk', count() > 0 FROM (EXPLAIN indexes = 1 SELECT sum(id) FROM t_keep_float WHERE a > [10.] SETTINGS use_skip_indexes = 0) WHERE explain ILIKE '%Granules: 1/2%';
SELECT 'keep_float_idx', count() > 0 FROM (EXPLAIN indexes = 1 SELECT sum(id) FROM t_keep_float WHERE a > [10.] SETTINGS use_primary_key = 0, use_skip_indexes = 1) WHERE explain ILIKE '%Granules: 1/2%';

-- top-level Nullable: the +Inf sentinel already orders it, so it must keep pruning
CREATE TABLE t_keep_nullable (id UInt64, a Nullable(String), INDEX i a TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 3, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_keep_nullable VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'x'), (5, NULL), (6, 'z');

SELECT 'keep_nullable_idx', count() > 0 FROM (EXPLAIN indexes = 1 SELECT sum(id) FROM t_keep_nullable WHERE a > 'w' SETTINGS use_skip_indexes = 1) WHERE explain ILIKE '%Granules: 1/2%';

-- Nullable(Tuple(String)): looking under the Nullable must not over-decline
CREATE TABLE t_keep_ntup (id UInt64, a Nullable(Tuple(String)))
ENGINE = MergeTree ORDER BY a
SETTINGS index_granularity = 3, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, allow_nullable_key = 1;
INSERT INTO t_keep_ntup VALUES (1, tuple('a')), (2, tuple('b')), (3, tuple('c')), (4, tuple('d')), (5, tuple('y')), (6, tuple('z'));

SELECT 'keep_ntup_pk', count() > 0 FROM (EXPLAIN indexes = 1 SELECT sum(id) FROM t_keep_ntup WHERE a > tuple('x')) WHERE explain ILIKE '%Granules: 1/2%';

-- a composite minmax index must keep pruning on its safe column
CREATE TABLE t_keep_comp (id UInt64, u UInt64, a Array(Nullable(String)), INDEX i (u, a) TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 3, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_keep_comp VALUES (1, 1, ['a']), (2, 2, [NULL]), (3, 3, ['c']), (4, 100, ['a']), (5, 200, [NULL]), (6, 300, ['c']);

SELECT 'keep_comp_idx', count() > 0 FROM (EXPLAIN indexes = 1 SELECT sum(id) FROM t_keep_comp WHERE u > 50 AND a > ['zzz'] SETTINGS use_skip_indexes = 1) WHERE explain ILIKE '%Granules: 1/2%';

-- the set index must keep pruning: it evaluates the predicate over its stored set
SELECT 'keep_set_idx', count() > 0 FROM (EXPLAIN indexes = 1 SELECT sum(id) FROM t_set WHERE a > ['w'] SETTINGS use_skip_indexes = 1, secondary_indices_enable_bulk_filtering = 0) WHERE explain ILIKE '%Granules: 1/2%';

DROP TABLE t_arr;
DROP TABLE t_map;
DROP TABLE t_part;
DROP TABLE t_partval;
DROP TABLE t_set;
DROP TABLE t_comp_in;
DROP TABLE t_json_proj;
DROP TABLE t_pk_arr;
DROP TABLE t_pk_tup;
DROP TABLE t_pk_ntup;
DROP TABLE t_keep_float;
DROP TABLE t_keep_nullable;
DROP TABLE t_keep_ntup;
DROP TABLE t_keep_comp;
