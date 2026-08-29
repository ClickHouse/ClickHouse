-- Complex types in VersionedCoalescingMergeTree. They are not Nullable, so for each of them
-- the whole value of the row with the maximum version wins.

SET optimize_on_insert = 0;

SELECT 'Array';

DROP TABLE IF EXISTS t_vcmt_array;

CREATE TABLE t_vcmt_array (key UInt64, version UInt64, arr Array(UInt64))
ENGINE = VersionedCoalescingMergeTree(version) ORDER BY key;

INSERT INTO t_vcmt_array VALUES (1, 2, [3, 4]), (2, 2, []);
INSERT INTO t_vcmt_array VALUES (1, 1, [1, 2]), (2, 1, [1]);

OPTIMIZE TABLE t_vcmt_array FINAL;
-- The empty array of the row with the maximum version is a value, it wins as well.
SELECT * FROM t_vcmt_array ORDER BY key;

DROP TABLE t_vcmt_array;

SELECT 'Map';

DROP TABLE IF EXISTS t_vcmt_map;

CREATE TABLE t_vcmt_map (key UInt64, version UInt64, m Map(String, UInt64))
ENGINE = VersionedCoalescingMergeTree(version) ORDER BY key;

INSERT INTO t_vcmt_map VALUES (1, 2, map('b', 2));
INSERT INTO t_vcmt_map VALUES (1, 1, map('a', 1));

OPTIMIZE TABLE t_vcmt_map FINAL;
-- No by-key merging of maps: the whole map of the row with the maximum version wins.
SELECT * FROM t_vcmt_map;

DROP TABLE t_vcmt_map;

SELECT 'Tuple';

DROP TABLE IF EXISTS t_vcmt_tuple_whole;

CREATE TABLE t_vcmt_tuple_whole (key UInt64, version UInt64, t Tuple(x Nullable(UInt64), y String))
ENGINE = VersionedCoalescingMergeTree(version) ORDER BY key;

INSERT INTO t_vcmt_tuple_whole VALUES (1, 2, (NULL, 'x2'));
INSERT INTO t_vcmt_tuple_whole VALUES (1, 1, (1, 'y1'));

OPTIMIZE TABLE t_vcmt_tuple_whole FINAL;
-- Without allow_tuple_element_aggregation the tuple is replaced as a whole,
-- its NULL elements are not coalesced independently.
SELECT * FROM t_vcmt_tuple_whole;

DROP TABLE t_vcmt_tuple_whole;

SELECT 'Tuple with element aggregation';

DROP TABLE IF EXISTS t_vcmt_tuple_flatten;

CREATE TABLE t_vcmt_tuple_flatten (key UInt64, version UInt64, data Tuple(x Nullable(UInt64), arr Array(UInt64)))
ENGINE = VersionedCoalescingMergeTree(version) ORDER BY key
SETTINGS allow_tuple_element_aggregation = 1;

INSERT INTO t_vcmt_tuple_flatten VALUES (1, 2, (NULL, [9]));
INSERT INTO t_vcmt_tuple_flatten VALUES (1, 1, (5, [1, 2]));

OPTIMIZE TABLE t_vcmt_tuple_flatten FINAL;
-- Each element is coalesced independently: the NULL element falls back to the older version,
-- the array element comes from the row with the maximum version.
SELECT * FROM t_vcmt_tuple_flatten;

DROP TABLE t_vcmt_tuple_flatten;

SELECT 'LowCardinality';

DROP TABLE IF EXISTS t_vcmt_lc;

CREATE TABLE t_vcmt_lc (key UInt64, version UInt64, s LowCardinality(Nullable(String)))
ENGINE = VersionedCoalescingMergeTree(version) ORDER BY key;

INSERT INTO t_vcmt_lc VALUES (1, 2, NULL), (2, 2, 'lc2');
INSERT INTO t_vcmt_lc VALUES (1, 1, 'lc1'), (2, 1, NULL);

OPTIMIZE TABLE t_vcmt_lc FINAL;
SELECT * FROM t_vcmt_lc ORDER BY key;

DROP TABLE t_vcmt_lc;

SELECT 'Dynamic';

DROP TABLE IF EXISTS t_vcmt_dynamic;

CREATE TABLE t_vcmt_dynamic (key UInt64, version UInt64, d Dynamic)
ENGINE = VersionedCoalescingMergeTree(version) ORDER BY key;

INSERT INTO t_vcmt_dynamic VALUES (1, 2, NULL), (2, 2, 'str');
INSERT INTO t_vcmt_dynamic VALUES (1, 1, 42), (2, 1, 123);

OPTIMIZE TABLE t_vcmt_dynamic FINAL;
-- NULL in a Dynamic column means "no value" as well.
SELECT key, version, d, dynamicType(d) FROM t_vcmt_dynamic ORDER BY key;

DROP TABLE t_vcmt_dynamic;
