-- https://github.com/ClickHouse/ClickHouse/issues/71899
-- `NaN` sorts after `+Inf` in ClickHouse, so it can appear in the primary key index as an
-- ordinary mark boundary. But in SQL every comparison with `NaN` is false, except `!=`,
-- which is always true. The primary key index must not skip granules containing `NaN`
-- for conditions that `NaN` satisfies, and it must agree with the plain filter.

DROP TABLE IF EXISTS t_nan_pk;
DROP TABLE IF EXISTS t_nan_no_pk;

-- `index_granularity = 1` puts `NaN` into the primary key index as a mark boundary.
CREATE TABLE t_nan_pk (c0 Float32) ENGINE = MergeTree ORDER BY c0 SETTINGS index_granularity = 1;
CREATE TABLE t_nan_no_pk (c0 Float32) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_nan_pk VALUES (1), (2), (nan);
INSERT INTO t_nan_no_pk VALUES (1), (2), (nan);

SELECT 'c0 != 1', (SELECT groupArraySorted(4)(toString(c0)) FROM t_nan_pk WHERE c0 != 1) AS indexed,
    indexed = (SELECT groupArraySorted(4)(toString(c0)) FROM t_nan_no_pk WHERE c0 != 1) AS same_as_plain_filter;
SELECT 'c0 = 1', (SELECT groupArraySorted(4)(toString(c0)) FROM t_nan_pk WHERE c0 = 1) AS indexed,
    indexed = (SELECT groupArraySorted(4)(toString(c0)) FROM t_nan_no_pk WHERE c0 = 1) AS same_as_plain_filter;
SELECT 'c0 < 2', (SELECT groupArraySorted(4)(toString(c0)) FROM t_nan_pk WHERE c0 < 2) AS indexed,
    indexed = (SELECT groupArraySorted(4)(toString(c0)) FROM t_nan_no_pk WHERE c0 < 2) AS same_as_plain_filter;
SELECT 'c0 > 1', (SELECT groupArraySorted(4)(toString(c0)) FROM t_nan_pk WHERE c0 > 1) AS indexed,
    indexed = (SELECT groupArraySorted(4)(toString(c0)) FROM t_nan_no_pk WHERE c0 > 1) AS same_as_plain_filter;
SELECT 'c0 >= 1', (SELECT groupArraySorted(4)(toString(c0)) FROM t_nan_pk WHERE c0 >= 1) AS indexed,
    indexed = (SELECT groupArraySorted(4)(toString(c0)) FROM t_nan_no_pk WHERE c0 >= 1) AS same_as_plain_filter;
SELECT 'c0 != nan', (SELECT groupArraySorted(4)(toString(c0)) FROM t_nan_pk WHERE c0 != nan) AS indexed,
    indexed = (SELECT groupArraySorted(4)(toString(c0)) FROM t_nan_no_pk WHERE c0 != nan) AS same_as_plain_filter;
SELECT 'c0 = nan', (SELECT groupArraySorted(4)(toString(c0)) FROM t_nan_pk WHERE c0 = nan) AS indexed,
    indexed = (SELECT groupArraySorted(4)(toString(c0)) FROM t_nan_no_pk WHERE c0 = nan) AS same_as_plain_filter;
SELECT 'c0 > nan', (SELECT groupArraySorted(4)(toString(c0)) FROM t_nan_pk WHERE c0 > nan) AS indexed,
    indexed = (SELECT groupArraySorted(4)(toString(c0)) FROM t_nan_no_pk WHERE c0 > nan) AS same_as_plain_filter;
SELECT 'isNaN(c0)', (SELECT groupArraySorted(4)(toString(c0)) FROM t_nan_pk WHERE isNaN(c0)) AS indexed,
    indexed = (SELECT groupArraySorted(4)(toString(c0)) FROM t_nan_no_pk WHERE isNaN(c0)) AS same_as_plain_filter;
SELECT 'NOT isNaN(c0)', (SELECT groupArraySorted(4)(toString(c0)) FROM t_nan_pk WHERE NOT isNaN(c0)) AS indexed,
    indexed = (SELECT groupArraySorted(4)(toString(c0)) FROM t_nan_no_pk WHERE NOT isNaN(c0)) AS same_as_plain_filter;
SELECT 'c0 IN (1, 2)', (SELECT groupArraySorted(4)(toString(c0)) FROM t_nan_pk WHERE c0 IN (1, 2)) AS indexed,
    indexed = (SELECT groupArraySorted(4)(toString(c0)) FROM t_nan_no_pk WHERE c0 IN (1, 2)) AS same_as_plain_filter;
SELECT 'c0 NOT IN (1)', (SELECT groupArraySorted(4)(toString(c0)) FROM t_nan_pk WHERE c0 NOT IN (1)) AS indexed,
    indexed = (SELECT groupArraySorted(4)(toString(c0)) FROM t_nan_no_pk WHERE c0 NOT IN (1)) AS same_as_plain_filter;

-- A constant that does not fit into `Float32` takes another code path in `KeyCondition`.
SELECT 'c0 != 1e300', (SELECT groupArraySorted(4)(toString(c0)) FROM t_nan_pk WHERE c0 != 1e300) AS indexed,
    indexed = (SELECT groupArraySorted(4)(toString(c0)) FROM t_nan_no_pk WHERE c0 != 1e300) AS same_as_plain_filter;

DROP TABLE t_nan_pk;
DROP TABLE t_nan_no_pk;

-- Many granules and many parts, so that the binary search over the primary key index is used.
DROP TABLE IF EXISTS t_nan_pk_large;
CREATE TABLE t_nan_pk_large (c0 Float64) ENGINE = MergeTree ORDER BY c0 SETTINGS index_granularity = 8;

INSERT INTO t_nan_pk_large SELECT number FROM numbers(1000);
INSERT INTO t_nan_pk_large SELECT if(number % 100 = 0, nan, number + 0.5) FROM numbers(1000);
INSERT INTO t_nan_pk_large VALUES (nan), (inf), (-inf);

SELECT count(), countIf(isNaN(c0)) FROM t_nan_pk_large;
SELECT count(), countIf(isNaN(c0)) FROM t_nan_pk_large WHERE c0 != 100;
SELECT count(), countIf(isNaN(c0)) FROM t_nan_pk_large WHERE c0 > 500;
SELECT count(), countIf(isNaN(c0)) FROM t_nan_pk_large WHERE c0 < 500;
SELECT count(), countIf(isNaN(c0)) FROM t_nan_pk_large WHERE c0 BETWEEN 100 AND 200;
SELECT count(), countIf(isNaN(c0)) FROM t_nan_pk_large WHERE c0 != nan;
SELECT count(), countIf(isNaN(c0)) FROM t_nan_pk_large WHERE NOT isNaN(c0);

OPTIMIZE TABLE t_nan_pk_large FINAL;

SELECT count(), countIf(isNaN(c0)) FROM t_nan_pk_large WHERE c0 != 100;
SELECT count(), countIf(isNaN(c0)) FROM t_nan_pk_large WHERE c0 > 500;

DROP TABLE t_nan_pk_large;

-- The original reproducer from the issue: the key column becomes `NaN` after aggregation.
DROP TABLE IF EXISTS t_nan_source;
DROP TABLE IF EXISTS t_nan_aggregated;

CREATE TABLE t_nan_source (c0 Int16, c1 UInt32) ENGINE = MergeTree ORDER BY c0;
INSERT INTO t_nan_source (c0, c1) VALUES (-2913, 7583471);
CREATE TABLE t_nan_aggregated ENGINE = MergeTree ORDER BY c0 AS (SELECT varSamp(c0) AS c0, c1 AS c1 FROM t_nan_source GROUP BY c1);

SELECT * FROM t_nan_aggregated;
SELECT c0, c1 FROM t_nan_aggregated WHERE c0 != -8.0324759543107315e+37;

DROP TABLE t_nan_source;
DROP TABLE t_nan_aggregated;
