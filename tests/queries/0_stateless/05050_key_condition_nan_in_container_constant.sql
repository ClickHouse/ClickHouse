-- A tuple or array constant that holds a NaN must not be used to skip parts or granules:
-- comparing a tuple against it follows IEEE rules, under which every comparison with the NaN
-- element is false, while an index range treats the NaN as greater than every finite value.

SET optimize_use_implicit_projections = 0;

-- Ground truth for the answers below, with no table involved.
SELECT 'truth order', (1, 1) < (nan, 1.), [1., 1.] < [nan, 1.];
SELECT 'truth equals', (nan, 1.) = (nan, 1.), [nan, 1.] = [nan, 1.];

DROP TABLE IF EXISTS t_partition;
DROP TABLE IF EXISTS t_partition_float;
DROP TABLE IF EXISTS t_primary_key;
DROP TABLE IF EXISTS t_skip_index;
DROP TABLE IF EXISTS t_nested;
DROP TABLE IF EXISTS t_array;
DROP TABLE IF EXISTS t_scalar;

CREATE TABLE t_partition (t Tuple(Int32, Int32)) ENGINE = MergeTree ORDER BY tuple() PARTITION BY t;
INSERT INTO t_partition VALUES ((1, 1)), ((2, 2));
SELECT 'partition less', count() FROM t_partition WHERE NOT (t < (nan, 1.));
SELECT 'partition lessOrEquals', count() FROM t_partition WHERE NOT (t <= (nan, 1.));
SELECT 'partition greater', count() FROM t_partition WHERE NOT (t > (nan, 1.));
SELECT 'partition equals', count() FROM t_partition WHERE t = (nan, 1.);

-- A floating point partition key is rejected on its own, but not inside a tuple.
CREATE TABLE t_partition_float (t Tuple(Float64, Int32)) ENGINE = MergeTree ORDER BY tuple() PARTITION BY t;
INSERT INTO t_partition_float VALUES ((1., 1)), ((2., 2));
SELECT 'partition float element', count() FROM t_partition_float WHERE NOT (t < (nan, 1.));

CREATE TABLE t_primary_key (t Tuple(Float64, Float64)) ENGINE = MergeTree ORDER BY t SETTINGS index_granularity = 1;
INSERT INTO t_primary_key VALUES ((1., 1.)), ((2., 2.)), ((3., 3.));
SELECT 'primary key', count() FROM t_primary_key WHERE NOT (t < (nan, 1.));

CREATE TABLE t_skip_index (x UInt32, t Tuple(Float64, Float64), INDEX i t TYPE minmax GRANULARITY 1)
    ENGINE = MergeTree ORDER BY x SETTINGS index_granularity = 1;
INSERT INTO t_skip_index VALUES (1, (1., 1.)), (2, (2., 2.)), (3, (3., 3.));
SELECT 'skip index', count() FROM t_skip_index WHERE NOT (t < (nan, 1.));

CREATE TABLE t_nested (t Tuple(Tuple(Float64, Float64), Float64)) ENGINE = MergeTree ORDER BY t SETTINGS index_granularity = 1;
INSERT INTO t_nested VALUES (((1., 1.), 1.)), (((2., 2.), 2.));
SELECT 'nested tuple', count() FROM t_nested WHERE NOT (t < ((nan, 1.), 1.));

-- Array comparison already agrees with the index range, so these answers must not change.
CREATE TABLE t_array (a Array(Float64)) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
INSERT INTO t_array VALUES ([1., 1.]), ([2., 2.]);
SELECT 'array less', count() FROM t_array WHERE NOT (a < [nan, 1.]);
SELECT 'array greater', count() FROM t_array WHERE NOT (a > [nan, 1.]);

-- A bare NaN constant was already handled, and stays handled.
CREATE TABLE t_scalar (i Int32) ENGINE = MergeTree ORDER BY tuple() PARTITION BY i;
INSERT INTO t_scalar VALUES (1), (2);
SELECT 'scalar', count() FROM t_scalar WHERE NOT (i < nan);

-- Constants that hold no NaN must still prune.
SELECT 'still prunes parts', count() > 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_partition WHERE NOT (t < (2, 2)))
WHERE explain ILIKE '%Parts: 1/2%';

SELECT 'still prunes granules', count() > 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_primary_key WHERE NOT (t < (3., 3.)))
WHERE explain ILIKE '%Granules: 2/3%';

DROP TABLE t_partition;
DROP TABLE t_partition_float;
DROP TABLE t_primary_key;
DROP TABLE t_skip_index;
DROP TABLE t_nested;
DROP TABLE t_array;
DROP TABLE t_scalar;
