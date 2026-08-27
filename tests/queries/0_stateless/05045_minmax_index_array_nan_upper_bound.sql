-- A minmax index on an array or map column must not skip a granule holding a NaN, because a NaN
-- element sorts after every number and such a value can satisfy the query.

DROP TABLE IF EXISTS t_arr;
CREATE TABLE t_arr (id UInt64, a Array(Float64), INDEX idx_a a TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 3, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_arr VALUES (1, [1.]), (2, [nan]), (3, [3.]);
INSERT INTO t_arr VALUES (4, [100.]), (5, [150.]), (6, [200.]);

SELECT 'array positive indexed', count() FROM t_arr WHERE a > [500.];
SELECT 'array positive no index', count() FROM t_arr WHERE a > [500.] SETTINGS use_skip_indexes = 0;
SELECT 'array positive prunes other granule', count() > 0 FROM (
    EXPLAIN indexes = 1 SELECT sum(id) FROM t_arr WHERE a > [500.]
    SETTINGS use_skip_indexes_on_data_read = 0, optimize_use_implicit_projections = 0
) WHERE explain LIKE '%Granules: 1/2%';

SELECT 'array negated indexed', count() FROM t_arr WHERE NOT (a <= [3.]);
SELECT 'array negated no index', count() FROM t_arr WHERE NOT (a <= [3.]) SETTINGS use_skip_indexes = 0;

-- With GRANULARITY 2 an index entry spans two marks, so the NaN must set the upper bound
-- whichever of the two marks it arrives in.
DROP TABLE IF EXISTS t_g2_first;
CREATE TABLE t_g2_first (id UInt64, a Array(Float64), INDEX idx_a a TYPE minmax GRANULARITY 2)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 3, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_g2_first VALUES (1, [1.]), (2, [nan]), (3, [3.]), (4, [100.]), (5, [150.]), (6, [200.]),
    (7, [10.]), (8, [20.]), (9, [30.]), (10, [40.]), (11, [50.]), (12, [60.]);

SELECT 'granularity 2 nan in first mark indexed', count() FROM t_g2_first WHERE a > [500.];
SELECT 'granularity 2 nan in first mark no index', count() FROM t_g2_first WHERE a > [500.] SETTINGS use_skip_indexes = 0;
SELECT 'granularity 2 nan in first mark prunes finite entry', count() > 0 FROM (
    EXPLAIN indexes = 1 SELECT sum(id) FROM t_g2_first WHERE a > [500.]
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
SELECT 'map still prunes', count() > 0 FROM (
    EXPLAIN indexes = 1 SELECT sum(id) FROM t_map WHERE a > map('k', 500.)
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

SELECT 'finite array still prunes', count() > 0 FROM (
    EXPLAIN indexes = 1 SELECT sum(id) FROM t_finite WHERE a > [150.]
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

DROP TABLE IF EXISTS t_bool;
CREATE TABLE t_bool (id UInt64, a Array(Tuple(Float64, Bool)), INDEX idx_a a TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 3, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_bool VALUES (1, [(1., true)]), (2, [(nan, true)]), (3, [(3., true)]);
INSERT INTO t_bool VALUES (4, [(100., true)]), (5, [(150., true)]), (6, [(200., true)]);

SELECT 'bool leaf indexed', count() FROM t_bool WHERE a > [(500., true)];
SELECT 'bool leaf no index', count() FROM t_bool WHERE a > [(500., true)] SETTINGS use_skip_indexes = 0;
SELECT 'bool leaf still prunes', count() > 0 FROM (
    EXPLAIN indexes = 1 SELECT sum(id) FROM t_bool WHERE a > [(500., true)]
    SETTINGS use_skip_indexes_on_data_read = 0, optimize_use_implicit_projections = 0
) WHERE explain LIKE '%Granules: 1/2%';

DROP TABLE t_arr;
DROP TABLE t_g2_first;
DROP TABLE t_g2_second;
DROP TABLE t_map;
DROP TABLE t_nullable;
DROP TABLE t_finite;
DROP TABLE t_str;
DROP TABLE t_mixed;
DROP TABLE t_bool;
