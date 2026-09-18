DROP TABLE IF EXISTS t_bf_array_in;

CREATE TABLE t_bf_array_in (x Array(UInt32), INDEX idx_x x TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 2;
INSERT INTO t_bf_array_in VALUES ([]), ([1]), ([2]), ([3]);

SELECT count() FROM t_bf_array_in WHERE x IN ([]) SETTINGS use_skip_indexes = 1;
SELECT count() FROM t_bf_array_in WHERE x IN ([]) SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_bf_array_in WHERE x IN ([], [2]) SETTINGS use_skip_indexes = 1;
SELECT count() FROM t_bf_array_in WHERE x IN ([], [2]) SETTINGS use_skip_indexes = 0;

-- A set with no empty array must keep pruning, so these must stay equal too.
SELECT count() FROM t_bf_array_in WHERE x IN ([2]) SETTINGS use_skip_indexes = 1;
SELECT count() FROM t_bf_array_in WHERE x IN ([2]) SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_bf_array_in WHERE x IN ([1], [3]) SETTINGS use_skip_indexes = 1;
SELECT count() FROM t_bf_array_in WHERE x IN ([1], [3]) SETTINGS use_skip_indexes = 0;

SELECT count() FROM t_bf_array_in WHERE x NOT IN ([], [2]) SETTINGS use_skip_indexes = 1;
SELECT count() FROM t_bf_array_in WHERE x NOT IN ([], [2]) SETTINGS use_skip_indexes = 0;

-- The set arrives from a subquery rather than a literal list.
SELECT count() FROM t_bf_array_in WHERE x IN (SELECT arrayFilter(i -> 0, [1])) SETTINGS use_skip_indexes = 1;
SELECT count() FROM t_bf_array_in WHERE x IN (SELECT arrayFilter(i -> 0, [1])) SETTINGS use_skip_indexes = 0;

DROP TABLE t_bf_array_in;

-- Every row of a granule is an empty array.
DROP TABLE IF EXISTS t_bf_array_in_all_empty;

CREATE TABLE t_bf_array_in_all_empty (x Array(UInt32), INDEX idx_x x TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 2;
INSERT INTO t_bf_array_in_all_empty VALUES ([]), ([]), ([5]), ([6]);

SELECT count() FROM t_bf_array_in_all_empty WHERE x IN ([], [5]) SETTINGS use_skip_indexes = 1;
SELECT count() FROM t_bf_array_in_all_empty WHERE x IN ([], [5]) SETTINGS use_skip_indexes = 0;

DROP TABLE t_bf_array_in_all_empty;

DROP TABLE IF EXISTS t_bf_array_in_string;

CREATE TABLE t_bf_array_in_string (x Array(String), INDEX idx_x x TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 2;
INSERT INTO t_bf_array_in_string VALUES ([]), (['a']), (['b']), (['c']);

SELECT count() FROM t_bf_array_in_string WHERE x IN ([], ['b']) SETTINGS use_skip_indexes = 1;
SELECT count() FROM t_bf_array_in_string WHERE x IN ([], ['b']) SETTINGS use_skip_indexes = 0;

DROP TABLE t_bf_array_in_string;

DROP TABLE IF EXISTS t_bf_array_in_low_cardinality;

CREATE TABLE t_bf_array_in_low_cardinality (x Array(LowCardinality(String)), INDEX idx_x x TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 2;
INSERT INTO t_bf_array_in_low_cardinality VALUES ([]), (['a']), (['b']), (['c']);

SELECT count() FROM t_bf_array_in_low_cardinality WHERE x IN ([], ['b']) SETTINGS use_skip_indexes = 1;
SELECT count() FROM t_bf_array_in_low_cardinality WHERE x IN ([], ['b']) SETTINGS use_skip_indexes = 0;

DROP TABLE t_bf_array_in_low_cardinality;

-- A tuple `IN` where the array component has an index of its own.
DROP TABLE IF EXISTS t_bf_array_in_tuple;

CREATE TABLE t_bf_array_in_tuple (x Array(UInt32), y UInt32,
  INDEX idx_x x TYPE bloom_filter GRANULARITY 1,
  INDEX idx_y y TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 2;
INSERT INTO t_bf_array_in_tuple VALUES ([], 1), ([1], 2), ([2], 3), ([3], 4);

SELECT count() FROM t_bf_array_in_tuple WHERE (x, y) IN (([], 1), ([2], 3)) SETTINGS use_skip_indexes = 1;
SELECT count() FROM t_bf_array_in_tuple WHERE (x, y) IN (([], 1), ([2], 3)) SETTINGS use_skip_indexes = 0;

DROP TABLE t_bf_array_in_tuple;

-- One index over both tuple components, so the array component shares an index condition with
-- its sibling. Either component order.
DROP TABLE IF EXISTS t_bf_array_in_tuple_one_index;

CREATE TABLE t_bf_array_in_tuple_one_index (x Array(UInt32), y UInt32,
  INDEX idx_xy (x, y) TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 2;
INSERT INTO t_bf_array_in_tuple_one_index VALUES ([], 1), ([1], 2), ([2], 3), ([3], 4);

SELECT count() FROM t_bf_array_in_tuple_one_index WHERE (x, y) IN (([], 1), ([2], 3)) SETTINGS use_skip_indexes = 1;
SELECT count() FROM t_bf_array_in_tuple_one_index WHERE (x, y) IN (([], 1), ([2], 3)) SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_bf_array_in_tuple_one_index WHERE (y, x) IN ((1, []), (3, [2])) SETTINGS use_skip_indexes = 1;
SELECT count() FROM t_bf_array_in_tuple_one_index WHERE (y, x) IN ((1, []), (3, [2])) SETTINGS use_skip_indexes = 0;

-- No empty array in the set, so the shared index keeps pruning.
SELECT count() FROM t_bf_array_in_tuple_one_index WHERE (x, y) IN (([1], 2), ([2], 3)) SETTINGS use_skip_indexes = 1;
SELECT count() FROM t_bf_array_in_tuple_one_index WHERE (x, y) IN (([1], 2), ([2], 3)) SETTINGS use_skip_indexes = 0;

DROP TABLE t_bf_array_in_tuple_one_index;

-- A set with no empty array still reaches the index and still prunes granules for an absent value.
-- How many granules survive is not asserted: that is a false positive draw over a granule count
-- that `index_granularity_bytes` randomizes.
DROP TABLE IF EXISTS t_bf_array_in_pruning;

CREATE TABLE t_bf_array_in_pruning (x Array(UInt32), y UInt32, INDEX idx_x x TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY y SETTINGS index_granularity = 8192;
INSERT INTO t_bf_array_in_pruning SELECT [number, number + 1000000], number FROM numbers(200000);

SELECT countIf(explain LIKE '%Name: idx_x%') > 0 AND countIf(toUInt64OrZero(g[1]) < toUInt64OrZero(g[2])) > 0
FROM (
    SELECT explain, splitByChar('/', extract(explain, 'Granules: ([0-9]+/[0-9]+)')) AS g
    FROM (EXPLAIN indexes = 1 SELECT sum(y) FROM t_bf_array_in_pruning WHERE x IN ([999999999, 999999998]))
);

DROP TABLE t_bf_array_in_pruning;
