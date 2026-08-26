-- Random settings limits: index_granularity=(8192, None)
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

-- An absent value is still pruned away, so the index keeps working.
DROP TABLE IF EXISTS t_bf_array_in_pruning;

CREATE TABLE t_bf_array_in_pruning (x Array(UInt32), y UInt32, INDEX idx_x x TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY y SETTINGS index_granularity = 8192;
INSERT INTO t_bf_array_in_pruning SELECT [number, number + 1000000], number FROM numbers(200000);

SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT sum(y) FROM t_bf_array_in_pruning WHERE x IN ([999999999, 999999998]))
WHERE explain ILIKE '%Granules: 0/%';

DROP TABLE t_bf_array_in_pruning;
