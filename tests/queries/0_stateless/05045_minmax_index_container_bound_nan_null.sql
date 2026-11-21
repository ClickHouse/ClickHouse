-- Skip indexes and the part-level minmax index of a partition key must not skip a granule or a part holding an
-- array or map value with a NaN or NULL element, because such an element sorts after every value and the value
-- can satisfy the query.

DROP TABLE IF EXISTS t_arr;
CREATE TABLE t_arr (id UInt64, a Array(Float64), INDEX idx_a a TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 3, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_arr VALUES (1, [1.]), (2, [nan]), (3, [3.]);
INSERT INTO t_arr VALUES (4, [100.]), (5, [150.]), (6, [200.]);

SELECT 'array positive indexed', count() FROM t_arr WHERE a > [500.];
SELECT 'array positive no index', count() FROM t_arr WHERE a > [500.] SETTINGS use_skip_indexes = 0;
SELECT 'array positive prunes other granule below', count() > 0 FROM (
    EXPLAIN indexes = 1 SELECT sum(id) FROM t_arr WHERE a < [50.]
    SETTINGS use_skip_indexes_on_data_read = 0, optimize_use_implicit_projections = 0
) WHERE explain LIKE '%Granules: 1/2%';

SELECT 'array negated indexed', count() FROM t_arr WHERE NOT (a <= [3.]);
SELECT 'array negated no index', count() FROM t_arr WHERE NOT (a <= [3.]) SETTINGS use_skip_indexes = 0;

-- With GRANULARITY 2 an index entry spans two marks, so the entry must be kept whichever of the two marks
-- the NaN arrives in.
DROP TABLE IF EXISTS t_g2_first;
CREATE TABLE t_g2_first (id UInt64, a Array(Float64), INDEX idx_a a TYPE minmax GRANULARITY 2)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 3, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_g2_first VALUES (1, [1.]), (2, [nan]), (3, [3.]), (4, [100.]), (5, [150.]), (6, [200.]),
    (7, [10.]), (8, [20.]), (9, [30.]), (10, [40.]), (11, [50.]), (12, [60.]);

SELECT 'granularity 2 nan in first mark indexed', count() FROM t_g2_first WHERE a > [500.];
SELECT 'granularity 2 nan in first mark no index', count() FROM t_g2_first WHERE a > [500.] SETTINGS use_skip_indexes = 0;
SELECT 'granularity 2 nan in first mark prunes finite entry below', count() > 0 FROM (
    EXPLAIN indexes = 1 SELECT sum(id) FROM t_g2_first WHERE a < [5.]
    SETTINGS use_skip_indexes_on_data_read = 0, optimize_use_implicit_projections = 0
) WHERE explain LIKE '%Granules: 2/4%';

DROP TABLE IF EXISTS t_g2_second;
CREATE TABLE t_g2_second (id UInt64, a Array(Float64), INDEX idx_a a TYPE minmax GRANULARITY 2)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 3, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_g2_second VALUES (1, [100.]), (2, [150.]), (3, [200.]), (4, [1.]), (5, [nan]), (6, [3.]);

SELECT 'granularity 2 nan in second mark indexed', count() FROM t_g2_second WHERE a > [500.];
SELECT 'granularity 2 nan in second mark no index', count() FROM t_g2_second WHERE a > [500.] SETTINGS use_skip_indexes = 0;

DROP TABLE IF EXISTS t_map;
CREATE TABLE t_map (id UInt64, a Map(String, Float64), INDEX idx_a a TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 3, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_map VALUES (1, map('k', 1.)), (2, map('k', nan)), (3, map('k', 3.));
INSERT INTO t_map VALUES (4, map('k', 100.)), (5, map('k', 150.)), (6, map('k', 200.));

SELECT 'map positive indexed', count() FROM t_map WHERE a > map('k', 500.);
SELECT 'map positive no index', count() FROM t_map WHERE a > map('k', 500.) SETTINGS use_skip_indexes = 0;
SELECT 'map still prunes below', count() > 0 FROM (
    EXPLAIN indexes = 1 SELECT sum(id) FROM t_map WHERE a < map('k', 50.)
    SETTINGS use_skip_indexes_on_data_read = 0, optimize_use_implicit_projections = 0
) WHERE explain LIKE '%Granules: 1/2%';

DROP TABLE IF EXISTS t_nullable;
CREATE TABLE t_nullable (id UInt64, a Array(Nullable(Float64)), INDEX idx_a a TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 3, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_nullable VALUES (1, [0.]), (2, [1., 10.]), (3, [1., NULL]);

SELECT 'nullable element indexed', count() FROM t_nullable WHERE a > [1., 5.];
SELECT 'nullable element no index', count() FROM t_nullable WHERE a > [1., 5.] SETTINGS use_skip_indexes = 0;

DROP TABLE IF EXISTS t_finite;
CREATE TABLE t_finite (id UInt64, a Array(Float64), INDEX idx_a a TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 3, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_finite VALUES (1, [1.]), (2, [2.]), (3, [3.]);
INSERT INTO t_finite VALUES (4, [100.]), (5, [150.]), (6, [200.]);

SELECT 'finite array still prunes below', count() > 0 FROM (
    EXPLAIN indexes = 1 SELECT sum(id) FROM t_finite WHERE a < [50.]
    SETTINGS use_skip_indexes_on_data_read = 0, optimize_use_implicit_projections = 0
) WHERE explain LIKE '%Granules: 1/2%';
SELECT 'finite array indexed', count() FROM t_finite WHERE a > [150.];
SELECT 'finite array no index', count() FROM t_finite WHERE a > [150.] SETTINGS use_skip_indexes = 0;

DROP TABLE IF EXISTS t_str;
CREATE TABLE t_str (id UInt64, a Array(String), INDEX idx_a a TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 3, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_str VALUES (1, ['a']), (2, ['b']), (3, ['c']);
INSERT INTO t_str VALUES (4, ['x']), (5, ['y']), (6, ['z']);

SELECT 'string array still prunes', count() > 0 FROM (
    EXPLAIN indexes = 1 SELECT sum(id) FROM t_str WHERE a > ['y']
    SETTINGS use_skip_indexes_on_data_read = 0, optimize_use_implicit_projections = 0
) WHERE explain LIKE '%Granules: 1/2%';
SELECT 'string array indexed', count() FROM t_str WHERE a > ['y'];
SELECT 'string array no index', count() FROM t_str WHERE a > ['y'] SETTINGS use_skip_indexes = 0;

DROP TABLE IF EXISTS t_mixed;
CREATE TABLE t_mixed (
    id UInt64,
    a Array(Tuple(Float64, String, UUID, LowCardinality(String), IPv6, Date)),
    INDEX idx_a a TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 3, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_mixed VALUES
    (1, [(1., 'a', '61f0c404-5cb3-11e7-907b-a6006ad3dba0', 'x', '::1', '2020-01-01')]),
    (2, [(nan, 'a', '61f0c404-5cb3-11e7-907b-a6006ad3dba0', 'x', '::1', '2020-01-01')]),
    (3, [(3., 'a', '61f0c404-5cb3-11e7-907b-a6006ad3dba0', 'x', '::1', '2020-01-01')]);
INSERT INTO t_mixed VALUES
    (4, [(100., 'a', '61f0c404-5cb3-11e7-907b-a6006ad3dba0', 'x', '::1', '2020-01-01')]),
    (5, [(150., 'a', '61f0c404-5cb3-11e7-907b-a6006ad3dba0', 'x', '::1', '2020-01-01')]),
    (6, [(200., 'a', '61f0c404-5cb3-11e7-907b-a6006ad3dba0', 'x', '::1', '2020-01-01')]);

SELECT 'mixed leaves indexed', count() FROM t_mixed
WHERE a > [(500., 'a', toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0'), toLowCardinality('x'), toIPv6('::1'), toDate('2020-01-01'))];
SELECT 'mixed leaves no index', count() FROM t_mixed
WHERE a > [(500., 'a', toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0'), toLowCardinality('x'), toIPv6('::1'), toDate('2020-01-01'))]
SETTINGS use_skip_indexes = 0;
SELECT 'mixed leaves still prunes below', count() > 0 FROM (
    EXPLAIN indexes = 1 SELECT sum(id) FROM t_mixed
    WHERE a < [(50., 'a', toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0'), toLowCardinality('x'), toIPv6('::1'), toDate('2020-01-01'))]
    SETTINGS use_skip_indexes_on_data_read = 0, optimize_use_implicit_projections = 0
) WHERE explain LIKE '%Granules: 1/2%';

DROP TABLE IF EXISTS t_bool;
CREATE TABLE t_bool (id UInt64, a Array(Tuple(Float64, Bool)), INDEX idx_a a TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 3, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_bool VALUES (1, [(1., true)]), (2, [(nan, true)]), (3, [(3., true)]);
INSERT INTO t_bool VALUES (4, [(100., true)]), (5, [(150., true)]), (6, [(200., true)]);

SELECT 'bool leaf indexed', count() FROM t_bool WHERE a > [(500., true)];
SELECT 'bool leaf no index', count() FROM t_bool WHERE a > [(500., true)] SETTINGS use_skip_indexes = 0;
SELECT 'bool leaf still prunes below', count() > 0 FROM (
    EXPLAIN indexes = 1 SELECT sum(id) FROM t_bool WHERE a < [(50., true)]
    SETTINGS use_skip_indexes_on_data_read = 0, optimize_use_implicit_projections = 0
) WHERE explain LIKE '%Granules: 1/2%';

-- A NULL element sorts after every value too.
DROP TABLE IF EXISTS t_null;
CREATE TABLE t_null (id UInt64, a Array(Nullable(String)), INDEX idx_a a TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 3, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_null VALUES (1, ['a']), (2, [NULL]), (3, ['c']);
INSERT INTO t_null VALUES (4, ['x']), (5, ['y']), (6, ['z']);
INSERT INTO t_null VALUES (7, ['n']), (8, ['o']), (9, ['p']);

SELECT 'skip null indexed', count() FROM t_null WHERE a > ['zzz'] SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0;
SELECT 'skip null no index', count() FROM t_null WHERE a > ['zzz'] SETTINGS use_skip_indexes = 0;
SELECT 'skip lower bound still prunes', count() > 0 FROM (
    EXPLAIN indexes = 1 SELECT sum(id) FROM t_null WHERE a < ['b']
    SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0, optimize_use_implicit_projections = 0
) WHERE explain LIKE '%Granules: 1/3%';

-- GRANULARITY 2 merges the bounds of two marks.
DROP TABLE IF EXISTS t_null_g2_first;
CREATE TABLE t_null_g2_first (id UInt64, a Array(Nullable(String)), INDEX idx_a a TYPE minmax GRANULARITY 2)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_null_g2_first VALUES (1, [NULL]), (2, ['c']);

DROP TABLE IF EXISTS t_null_g2_second;
CREATE TABLE t_null_g2_second (id UInt64, a Array(Nullable(String)), INDEX idx_a a TYPE minmax GRANULARITY 2)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_null_g2_second VALUES (1, ['c']), (2, [NULL]);

SELECT 'skip null merge first indexed', count() FROM t_null_g2_first WHERE a > ['zzz'] SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0;
SELECT 'skip null merge first no index', count() FROM t_null_g2_first WHERE a > ['zzz'] SETTINGS use_skip_indexes = 0;
SELECT 'skip null merge second indexed', count() FROM t_null_g2_second WHERE a > ['zzz'] SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0;
SELECT 'skip null merge second no index', count() FROM t_null_g2_second WHERE a > ['zzz'] SETTINGS use_skip_indexes = 0;

DROP TABLE IF EXISTS t_null_map;
CREATE TABLE t_null_map (id UInt64, a Map(String, Nullable(String)), INDEX idx_a a TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 3, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_null_map VALUES (1, map('k', 'a')), (2, map('k', NULL)), (3, map('k', 'c'));
INSERT INTO t_null_map VALUES (4, map('k', 'x')), (5, map('k', 'y')), (6, map('k', 'z'));

SELECT 'skip map indexed', count() FROM t_null_map WHERE a > map('k', 'zzz') SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0;
SELECT 'skip map no index', count() FROM t_null_map WHERE a > map('k', 'zzz') SETTINGS use_skip_indexes = 0;

DROP TABLE IF EXISTS t_null_lc;
CREATE TABLE t_null_lc (id UInt64, a Array(LowCardinality(Nullable(String))), INDEX idx_a a TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 3, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_null_lc VALUES (1, ['a']), (2, [NULL]), (3, ['c']);
INSERT INTO t_null_lc VALUES (4, ['x']), (5, ['y']), (6, ['z']);

SELECT 'skip lc indexed', count() FROM t_null_lc WHERE a > ['zzz'] SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0;
SELECT 'skip lc no index', count() FROM t_null_lc WHERE a > ['zzz'] SETTINGS use_skip_indexes = 0;

DROP TABLE IF EXISTS t_null_tuple;
CREATE TABLE t_null_tuple (id UInt64, t Tuple(Array(Nullable(String)), UInt8), INDEX idx_t t TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 3, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_null_tuple VALUES (1, (['a'], 1)), (2, ([NULL], 1)), (3, (['c'], 1));

SELECT 'skip tuple indexed', count() FROM t_null_tuple WHERE t > (['zzz'], 0) SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0;
SELECT 'skip tuple no index', count() FROM t_null_tuple WHERE t > (['zzz'], 0) SETTINGS use_skip_indexes = 0;

DROP TABLE IF EXISTS t_nan_tuple;
CREATE TABLE t_nan_tuple (id UInt64, t Tuple(Array(Float64), UInt8), INDEX idx_t t TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 3, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_nan_tuple VALUES (1, ([1.], 1)), (2, ([nan], 1)), (3, ([3.], 1));

SELECT 'skip tuple nan indexed', count() FROM t_nan_tuple WHERE t > ([500.], 0) SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0;
SELECT 'skip tuple nan no index', count() FROM t_nan_tuple WHERE t > ([500.], 0) SETTINGS use_skip_indexes = 0;

DROP TABLE IF EXISTS t_null_nan;
CREATE TABLE t_null_nan (id UInt64, a Array(Nullable(Float64)), INDEX idx_a a TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 3, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_null_nan VALUES (1, [1.]), (2, [nan]), (3, [NULL]);
INSERT INTO t_null_nan VALUES (4, [100.]), (5, [150.]), (6, [200.]);

SELECT 'skip null and nan indexed', count() FROM t_null_nan WHERE a > [500.] SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0;
SELECT 'skip null and nan no index', count() FROM t_null_nan WHERE a > [500.] SETTINGS use_skip_indexes = 0;

-- A lower bound merged across marks can hold a NULL that sorts above a row of the entry.
DROP TABLE IF EXISTS t_null_in;
CREATE TABLE t_null_in (id UInt64, a Array(Nullable(Float64)), INDEX idx_a a TYPE minmax GRANULARITY 2)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_null_in VALUES (1, [1., NULL]), (2, [1., 5.]);

SELECT 'skip in merged indexed', count() FROM t_null_in WHERE a IN ([1., 5.]) SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0;
SELECT 'skip in merged no index', count() FROM t_null_in WHERE a IN ([1., 5.]) SETTINGS use_skip_indexes = 0;
SELECT 'skip in merged above indexed', count() FROM t_null_in WHERE a > [1., 6.] SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0;
SELECT 'skip in merged above no index', count() FROM t_null_in WHERE a > [1., 6.] SETTINGS use_skip_indexes = 0;
SELECT 'skip in merged equal indexed', count() FROM t_null_in WHERE a = [1., 5.] SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0;
SELECT 'skip in merged equal no index', count() FROM t_null_in WHERE a = [1., 5.] SETTINGS use_skip_indexes = 0;
SELECT 'skip in merged below a null constant indexed', count() FROM t_null_in WHERE a < [1., NULL, 2.] SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0;
SELECT 'skip in merged below a null constant no index', count() FROM t_null_in WHERE a < [1., NULL, 2.] SETTINGS use_skip_indexes = 0;
SELECT 'skip in merged null constant indexed', count() FROM t_null_in WHERE a IN ([1., NULL]) SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0;
SELECT 'skip in merged null constant no index', count() FROM t_null_in WHERE a IN ([1., NULL]) SETTINGS use_skip_indexes = 0;

-- The set index reads its bounds from the stored values, then evaluates the values themselves.
DROP TABLE IF EXISTS t_set_null;
CREATE TABLE t_set_null (id UInt64, a Array(Nullable(String)), INDEX idx_a a TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 3, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_set_null VALUES (1, ['a']), (2, [NULL]), (3, ['c']);
INSERT INTO t_set_null VALUES (4, ['x']), (5, ['y']), (6, ['z']);

SELECT 'set null indexed', count() FROM t_set_null WHERE a > ['zzz']
SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0, secondary_indices_enable_bulk_filtering = 0;
SELECT 'set null bulk filtering', count() FROM t_set_null WHERE a > ['zzz']
SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0, secondary_indices_enable_bulk_filtering = 1;
SELECT 'set null no index', count() FROM t_set_null WHERE a > ['zzz'] SETTINGS use_skip_indexes = 0;
SELECT 'set null still prunes', count() > 0 FROM (
    EXPLAIN indexes = 1 SELECT sum(id) FROM t_set_null WHERE a > ['zzz']
    SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0, secondary_indices_enable_bulk_filtering = 0,
        optimize_use_implicit_projections = 0
) WHERE explain LIKE '%Granules: 1/2%';

DROP TABLE IF EXISTS t_set_nan;
CREATE TABLE t_set_nan (id UInt64, a Array(Float64), INDEX idx_a a TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 3, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_set_nan VALUES (1, [1.]), (2, [nan]), (3, [3.]);
INSERT INTO t_set_nan VALUES (4, [100.]), (5, [150.]), (6, [200.]);

SELECT 'set nan indexed', count() FROM t_set_nan WHERE a > [500.]
SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0, secondary_indices_enable_bulk_filtering = 0;
SELECT 'set nan no index', count() FROM t_set_nan WHERE a > [500.] SETTINGS use_skip_indexes = 0;

-- The part-level minmax index of a partition key.
DROP TABLE IF EXISTS t_part_null;
CREATE TABLE t_part_null (id UInt64, a Array(Nullable(String))) ENGINE = MergeTree PARTITION BY length(a) ORDER BY id;
INSERT INTO t_part_null VALUES (1, ['a']), (2, [NULL]), (3, ['c']), (4, ['x', 'y']), (5, ['x', 'z']);

SELECT 'part null', count() FROM t_part_null WHERE a > ['zzz']
SETTINGS use_partition_pruning = 1, optimize_use_implicit_projections = 0;
SELECT 'part null no partition pruning', count() FROM t_part_null WHERE a > ['zzz']
SETTINGS use_partition_pruning = 0, use_partition_minmax_for_primary_key_pruning = 0;
SELECT 'part null still prunes below', count() > 0 FROM (
    EXPLAIN indexes = 1 SELECT sum(id) FROM t_part_null WHERE a < ['b']
    SETTINGS use_partition_pruning = 1, optimize_use_implicit_projections = 0
) WHERE explain LIKE '%Parts: 1/2%';

DROP TABLE IF EXISTS t_part_nan;
CREATE TABLE t_part_nan (id UInt64, a Array(Float64)) ENGINE = MergeTree PARTITION BY length(a) ORDER BY id;
INSERT INTO t_part_nan VALUES (1, [1.]), (2, [nan]), (3, [3.]), (4, [100., 1.]), (5, [150., 1.]);

SELECT 'part nan', count() FROM t_part_nan WHERE a > [500.]
SETTINGS use_partition_pruning = 1, optimize_use_implicit_projections = 0;
SELECT 'part nan no partition pruning', count() FROM t_part_nan WHERE a > [500.]
SETTINGS use_partition_pruning = 0, use_partition_minmax_for_primary_key_pruning = 0;
SELECT 'part nan still prunes below', count() > 0 FROM (
    EXPLAIN indexes = 1 SELECT sum(id) FROM t_part_nan WHERE a < [50.]
    SETTINGS use_partition_pruning = 1, optimize_use_implicit_projections = 0
) WHERE explain LIKE '%Parts: 1/2%';

-- A merge combines the stored bounds of its source parts.
DROP TABLE IF EXISTS t_part_merge;
CREATE TABLE t_part_merge (id UInt64, a Array(Nullable(String))) ENGINE = MergeTree PARTITION BY length(a) ORDER BY id;
SYSTEM STOP MERGES t_part_merge;
INSERT INTO t_part_merge VALUES (1, [NULL]);
INSERT INTO t_part_merge VALUES (2, ['c']);
SYSTEM START MERGES t_part_merge;
OPTIMIZE TABLE t_part_merge FINAL;

SELECT 'part merge active parts', count() FROM system.parts WHERE database = currentDatabase() AND table = 't_part_merge' AND active;
SELECT 'part merge', count() FROM t_part_merge WHERE a > ['zzz']
SETTINGS use_partition_pruning = 1, optimize_use_implicit_projections = 0;
SELECT 'part merge no partition pruning', count() FROM t_part_merge WHERE a > ['zzz']
SETTINGS use_partition_pruning = 0, use_partition_minmax_for_primary_key_pruning = 0;

DROP TABLE IF EXISTS t_part_merge_in;
CREATE TABLE t_part_merge_in (id UInt64, a Array(Nullable(Float64))) ENGINE = MergeTree PARTITION BY length(a) ORDER BY id;
SYSTEM STOP MERGES t_part_merge_in;
INSERT INTO t_part_merge_in VALUES (1, [1., NULL]);
INSERT INTO t_part_merge_in VALUES (2, [1., 5.]);
SYSTEM START MERGES t_part_merge_in;
OPTIMIZE TABLE t_part_merge_in FINAL;

SELECT 'part merge in active parts', count() FROM system.parts WHERE database = currentDatabase() AND table = 't_part_merge_in' AND active;
SELECT 'part merge in', count() FROM t_part_merge_in WHERE a IN ([1., 5.])
SETTINGS use_partition_pruning = 1, optimize_use_implicit_projections = 0;
SELECT 'part merge in no partition pruning', count() FROM t_part_merge_in WHERE a IN ([1., 5.])
SETTINGS use_partition_pruning = 0, use_partition_minmax_for_primary_key_pruning = 0;
SELECT 'part merge in above', count() FROM t_part_merge_in WHERE a > [1., 6.]
SETTINGS use_partition_pruning = 1, optimize_use_implicit_projections = 0;
SELECT 'part merge in above no partition pruning', count() FROM t_part_merge_in WHERE a > [1., 6.]
SETTINGS use_partition_pruning = 0, use_partition_minmax_for_primary_key_pruning = 0;

-- Primary key analysis can take the part-level bound of a key column that is not loaded in memory.
DROP TABLE IF EXISTS t_pk_part;
CREATE TABLE t_pk_part (k UInt64, a Array(Nullable(String))) ENGINE = MergeTree PARTITION BY length(a) ORDER BY (k, a)
SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, allow_nullable_key = 1,
    primary_key_ratio_of_unique_prefix_values_to_skip_suffix_columns = 0.5;
INSERT INTO t_pk_part VALUES (1, ['a']), (2, [NULL]), (3, ['c']);

SELECT 'pk partition bound', count() FROM t_pk_part WHERE a > ['zzz'];
SELECT 'pk partition bound no partition pruning', count() FROM t_pk_part WHERE a > ['zzz'] SETTINGS use_partition_pruning = 0;
SELECT 'pk partition bound no index', count() FROM t_pk_part WHERE a > ['zzz'] SETTINGS use_partition_pruning = 0, use_primary_key = 0;

-- `_minmax_count_projection` serves the stored part bounds as `min` and `max`.
SELECT 'minmax_count null uses projection', count() > 0 FROM (
    EXPLAIN SELECT min(a), max(a) FROM t_part_null SETTINGS optimize_use_projections = 1, optimize_use_implicit_projections = 1
) WHERE explain LIKE '%_minmax_count_projection%';
SELECT 'minmax_count null', min(a), max(a) FROM t_part_null SETTINGS optimize_use_projections = 1, optimize_use_implicit_projections = 1;
SELECT 'minmax_count null no projection', min(a), max(a) FROM t_part_null SETTINGS optimize_use_implicit_projections = 0;
SELECT 'minmax_count nan uses projection', count() > 0 FROM (
    EXPLAIN SELECT min(a), max(a) FROM t_part_nan SETTINGS optimize_use_projections = 1, optimize_use_implicit_projections = 1
) WHERE explain LIKE '%_minmax_count_projection%';
SELECT 'minmax_count nan', min(a), max(a) FROM t_part_nan SETTINGS optimize_use_projections = 1, optimize_use_implicit_projections = 1;
SELECT 'minmax_count nan no projection', min(a), max(a) FROM t_part_nan SETTINGS optimize_use_implicit_projections = 0;

DROP TABLE t_arr;
DROP TABLE t_g2_first;
DROP TABLE t_g2_second;
DROP TABLE t_map;
DROP TABLE t_nullable;
DROP TABLE t_finite;
DROP TABLE t_str;
DROP TABLE t_mixed;
DROP TABLE t_bool;
DROP TABLE t_null;
DROP TABLE t_null_g2_first;
DROP TABLE t_null_g2_second;
DROP TABLE t_null_map;
DROP TABLE t_null_lc;
DROP TABLE t_null_tuple;
DROP TABLE t_nan_tuple;
DROP TABLE t_null_nan;
DROP TABLE t_null_in;
DROP TABLE t_set_null;
DROP TABLE t_set_nan;
DROP TABLE t_part_null;
DROP TABLE t_part_nan;
DROP TABLE t_part_merge;
DROP TABLE t_part_merge_in;
DROP TABLE t_pk_part;
