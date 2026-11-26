-- A `Tuple` comparison answers NULL once it reaches a NULL element: `(2, NULL) >= (2, 3)` is NULL. The key
-- stores such a row between marks that hold no NULL themselves, e.g. `(2, NULL)` between `(2, 3)` and
-- `(3, 1)`, so no granule can be claimed to match a comparison wholly. The exact count from the primary
-- key used to count such rows.
-- https://github.com/ClickHouse/ClickHouse/issues/124155

SET use_query_condition_cache = 0;
-- The exact count from the primary key is `_exact_count_projection`, an implicit projection.
SET optimize_use_projections = 1, optimize_use_implicit_projections = 1;

DROP TABLE IF EXISTS t_exact_count_tuple_nullable;

SELECT 'a descending key';

CREATE TABLE t_exact_count_tuple_nullable (t Tuple(UInt8, Nullable(UInt8))) ENGINE = MergeTree ORDER BY t DESC
SETTINGS index_granularity = 2, index_granularity_bytes = '10Mi', allow_nullable_key = 1;

INSERT INTO t_exact_count_tuple_nullable VALUES ((1, 5)), ((2, 3)), ((2, NULL)), ((3, 1)), ((3, NULL)), ((4, 1));

SELECT count() FROM t_exact_count_tuple_nullable WHERE t >= (2, 3);
SELECT count() FROM t_exact_count_tuple_nullable WHERE t >= (2, 3) SETTINGS optimize_use_projections = 0;

DROP TABLE t_exact_count_tuple_nullable;

-- No mark holds the NULL here, so the ascending key is affected as well.
SELECT 'an ascending key';

CREATE TABLE t_exact_count_tuple_nullable (t Tuple(UInt8, Nullable(UInt8))) ENGINE = MergeTree ORDER BY t
SETTINGS index_granularity = 2, index_granularity_bytes = '10Mi', allow_nullable_key = 1;

INSERT INTO t_exact_count_tuple_nullable VALUES ((1, 5)), ((1, 6)), ((2, 3)), ((2, NULL)), ((3, 1)), ((4, 1));

SELECT count() FROM t_exact_count_tuple_nullable WHERE t >= (2, 3);
SELECT count() FROM t_exact_count_tuple_nullable WHERE NOT (t < (2, 3));
SELECT count() FROM t_exact_count_tuple_nullable WHERE t >= (2, 3) SETTINGS optimize_use_projections = 0;

-- The comparison is decided before it reaches the NULL, so `(2, NULL)` is counted.
SELECT count() FROM t_exact_count_tuple_nullable WHERE t > (1, 9);
-- `IN` answers 0 or 1 for a row holding a NULL.
SELECT count() FROM t_exact_count_tuple_nullable WHERE t IN ((2, 3), (3, 1));
SELECT count() FROM t_exact_count_tuple_nullable WHERE t NOT IN ((1, 5), (1, 6));

DROP TABLE t_exact_count_tuple_nullable;

SELECT 'a NULL in a nested tuple';

CREATE TABLE t_exact_count_tuple_nullable (t Tuple(UInt8, Tuple(UInt8, Nullable(UInt8)))) ENGINE = MergeTree ORDER BY t
SETTINGS index_granularity = 2, index_granularity_bytes = '10Mi', allow_nullable_key = 1;

INSERT INTO t_exact_count_tuple_nullable VALUES ((1, (1, 5))), ((1, (1, 6))), ((2, (3, 3))), ((2, (3, NULL))), ((3, (1, 1))), ((4, (1, 1)));

SELECT count() FROM t_exact_count_tuple_nullable WHERE t >= (2, (3, 3));
SELECT count() FROM t_exact_count_tuple_nullable WHERE t >= (2, (3, 3)) SETTINGS optimize_use_projections = 0;

DROP TABLE t_exact_count_tuple_nullable;

SELECT 'a LowCardinality(Nullable) element';

SET allow_suspicious_low_cardinality_types = 1;

CREATE TABLE t_exact_count_tuple_nullable (t Tuple(UInt8, LowCardinality(Nullable(UInt8)))) ENGINE = MergeTree ORDER BY t
SETTINGS index_granularity = 2, index_granularity_bytes = '10Mi', allow_nullable_key = 1;

INSERT INTO t_exact_count_tuple_nullable VALUES ((1, 5)), ((1, 6)), ((2, 3)), ((2, NULL)), ((3, 1)), ((4, 1));

SELECT count() FROM t_exact_count_tuple_nullable WHERE t >= (2, 3);
SELECT count() FROM t_exact_count_tuple_nullable WHERE t >= (2, 3) SETTINGS optimize_use_projections = 0;

DROP TABLE t_exact_count_tuple_nullable;

-- A NULL tuple was already handled for a `Nullable(Tuple)` key, a NULL element inside a tuple was not.
SELECT 'a Nullable(Tuple) key';

SET enable_nullable_tuple_type = 1;

CREATE TABLE t_exact_count_tuple_nullable (t Nullable(Tuple(UInt8, Nullable(UInt8)))) ENGINE = MergeTree ORDER BY t
SETTINGS index_granularity = 1, index_granularity_bytes = '10Mi', allow_nullable_key = 1;

INSERT INTO t_exact_count_tuple_nullable VALUES ((1, 5)), ((1, 6)), ((2, 3)), ((2, NULL)), ((3, 1)), ((4, 1)), (NULL);

SELECT count() FROM t_exact_count_tuple_nullable WHERE t >= (2, 3);
SELECT count() FROM t_exact_count_tuple_nullable WHERE t >= (2, 3) SETTINGS optimize_use_projections = 0;

DROP TABLE t_exact_count_tuple_nullable;
