-- A tuple/array leaf that is in the sorting key must not be aggregated during a Coalescing merge:
-- it is a key column and must be preserved, exactly like a scalar key column. Only the non-key
-- leaves of the tuple are coalesced.

DROP TABLE IF EXISTS t_key_tuple;

CREATE TABLE t_key_tuple
(
    pk Tuple(arr Array(UInt64), name String),
    v UInt64
)
ENGINE = CoalescingMergeTree
ORDER BY pk.arr
SETTINGS allow_tuple_element_aggregation = 1;

INSERT INTO t_key_tuple VALUES (([1], 'a'), 10);
INSERT INTO t_key_tuple VALUES (([1], 'b'), 20);
INSERT INTO t_key_tuple VALUES (([2], 'c'), 30);

OPTIMIZE TABLE t_key_tuple FINAL;

-- `pk.arr` is the sorting key and is preserved per group; `pk.name` and `v` are coalesced.
SELECT pk.arr, pk.name, v FROM t_key_tuple ORDER BY pk.arr;

DROP TABLE t_key_tuple;

-- The same exclusion applies to a tuple leaf that is referenced by the partition key: it is a
-- key column and must be preserved, not coalesced.
DROP TABLE IF EXISTS t_part_key_tuple;

CREATE TABLE t_part_key_tuple
(
    pk Tuple(part UInt64, name String),
    k UInt64,
    v UInt64
)
ENGINE = CoalescingMergeTree
PARTITION BY pk.part
ORDER BY k
SETTINGS allow_tuple_element_aggregation = 1;

INSERT INTO t_part_key_tuple VALUES ((1, 'a'), 100, 10);
INSERT INTO t_part_key_tuple VALUES ((1, 'b'), 100, 20);
INSERT INTO t_part_key_tuple VALUES ((2, 'c'), 200, 30);

OPTIMIZE TABLE t_part_key_tuple FINAL;

-- `pk.part` is the partition key and is preserved per group; `pk.name` and `v` are coalesced.
SELECT pk.part, pk.name, k, v FROM t_part_key_tuple ORDER BY pk.part;

DROP TABLE t_part_key_tuple;
