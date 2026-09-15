-- `optimize_read_in_order` serves `ORDER BY negate(x)` over an ascending `Float` key with a backward read,
-- which surfaces the `NaN`s first while `ASC NULLS LAST` wants them last. The guard that keeps floats with
-- `NaN` out of the optimization tested `nulls_direction` before the direction of the match was known, so it
-- passed and the elided sort never repaired the placement.

DROP TABLE IF EXISTS t_read_in_order_nan;
CREATE TABLE t_read_in_order_nan (x Float64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_read_in_order_nan VALUES (1), (2), (nan), (3);

SELECT 'a monotonically decreasing function of the key';
SELECT negate(x) AS n FROM t_read_in_order_nan ORDER BY n;
SELECT negate(x) AS n FROM t_read_in_order_nan ORDER BY n SETTINGS optimize_read_in_order = 0;

SELECT 'under LIMIT, where the wrong order also returns wrong rows';
SELECT negate(x) AS n FROM t_read_in_order_nan ORDER BY n LIMIT 2;
SELECT negate(x) AS n FROM t_read_in_order_nan ORDER BY n LIMIT 2 SETTINGS optimize_read_in_order = 0;

-- The order a backward read satisfies exactly: the values ascend and the NaNs come first, which is where
-- a backward read of the key puts them. The old guard rejected every `nulls_direction = -1` request for a
-- float key, so this one was sorted; it is now read in order, and the prefix says so.
SELECT 'the order a backward read satisfies exactly';
SELECT negate(x) AS n FROM t_read_in_order_nan ORDER BY n ASC NULLS FIRST;
SELECT negate(x) AS n FROM t_read_in_order_nan ORDER BY n ASC NULLS FIRST SETTINGS optimize_read_in_order = 0;
SELECT count() > 0 AS read_in_order_used FROM (
    EXPLAIN actions = 1 SELECT negate(x) AS n FROM t_read_in_order_nan ORDER BY n ASC NULLS FIRST
    SETTINGS optimize_read_in_order = 1)
WHERE explain LIKE '%Prefix sort description%';

SELECT 'and the opposite one, which neither read direction satisfies';
SELECT negate(x) AS n FROM t_read_in_order_nan ORDER BY n DESC NULLS FIRST;
SELECT negate(x) AS n FROM t_read_in_order_nan ORDER BY n DESC NULLS FIRST SETTINGS optimize_read_in_order = 0;

SELECT 'the key itself, which was always read in order';
SELECT x FROM t_read_in_order_nan ORDER BY x;
SELECT x FROM t_read_in_order_nan ORDER BY x DESC;
SELECT x FROM t_read_in_order_nan ORDER BY x SETTINGS optimize_read_in_order = 0;

SELECT 'an increasing function of the key';
SELECT x + 1 AS n FROM t_read_in_order_nan ORDER BY n;
SELECT x + 1 AS n FROM t_read_in_order_nan ORDER BY n SETTINGS optimize_read_in_order = 0;

SELECT 'and a key column declared DESC';
DROP TABLE IF EXISTS t_read_in_order_nan_desc;
CREATE TABLE t_read_in_order_nan_desc (x Float64) ENGINE = MergeTree ORDER BY x DESC;
INSERT INTO t_read_in_order_nan_desc VALUES (1), (2), (nan), (3);
SELECT negate(x) AS n FROM t_read_in_order_nan_desc ORDER BY n;
SELECT negate(x) AS n FROM t_read_in_order_nan_desc ORDER BY n SETTINGS optimize_read_in_order = 0;
SELECT x FROM t_read_in_order_nan_desc ORDER BY x DESC;
SELECT x FROM t_read_in_order_nan_desc ORDER BY x DESC SETTINGS optimize_read_in_order = 0;

DROP TABLE t_read_in_order_nan_desc;
DROP TABLE t_read_in_order_nan;

-- A later key column can fail the placement check on its own, with both keys read in the same
-- direction: key `(a, b)` read forward puts the `NaN`s of `b` last, while `b ASC NULLS FIRST` wants them
-- first. The check now runs after the key was consumed, so the rejection has to give the key back -
-- otherwise the rejected column would still widen `used_prefix_of_sorting_key_size`, which is the width
-- the storage reads with and applies a hard `LIMIT` to, while the advertised prefix is only `a`.
SELECT 'a later key rejected on its own';
DROP TABLE IF EXISTS t_read_in_order_nan_two_keys;
CREATE TABLE t_read_in_order_nan_two_keys (a Float64, b Float64) ENGINE = MergeTree ORDER BY (a, b);
INSERT INTO t_read_in_order_nan_two_keys VALUES (1, 1), (1, 2), (1, nan), (1, 3);
INSERT INTO t_read_in_order_nan_two_keys VALUES (1, 0), (1, 5), (2, 1);
SELECT a, b FROM t_read_in_order_nan_two_keys ORDER BY a ASC, b ASC NULLS FIRST;
SELECT a, b FROM t_read_in_order_nan_two_keys ORDER BY a ASC, b ASC NULLS FIRST SETTINGS optimize_read_in_order = 0;
SELECT a, b FROM t_read_in_order_nan_two_keys ORDER BY a ASC, b ASC NULLS FIRST LIMIT 3;
SELECT a, b FROM t_read_in_order_nan_two_keys ORDER BY a ASC, b ASC NULLS FIRST LIMIT 3 SETTINGS optimize_read_in_order = 0;
SELECT a, negate(b) AS n FROM t_read_in_order_nan_two_keys ORDER BY a, n LIMIT 1;
SELECT a, negate(b) AS n FROM t_read_in_order_nan_two_keys ORDER BY a, n LIMIT 1 SETTINGS optimize_read_in_order = 0;
DROP TABLE t_read_in_order_nan_two_keys;

-- A `LowCardinality(Float64)` key holds its `NaN`s at a physical end just like a plain one, so the guard
-- has to see through `LowCardinality`.
SELECT 'a LowCardinality float key';
DROP TABLE IF EXISTS t_read_in_order_nan_lc;
CREATE TABLE t_read_in_order_nan_lc (x LowCardinality(Float64)) ENGINE = MergeTree ORDER BY x
SETTINGS allow_suspicious_low_cardinality_types = 1;
INSERT INTO t_read_in_order_nan_lc VALUES (1), (2), (nan), (3);
SELECT negate(x) AS n FROM t_read_in_order_nan_lc ORDER BY n;
SELECT negate(x) AS n FROM t_read_in_order_nan_lc ORDER BY n SETTINGS optimize_read_in_order = 0;
SELECT negate(x) AS n FROM t_read_in_order_nan_lc ORDER BY n LIMIT 2;
SELECT negate(x) AS n FROM t_read_in_order_nan_lc ORDER BY n LIMIT 2 SETTINGS optimize_read_in_order = 0;
DROP TABLE t_read_in_order_nan_lc;

-- `query_plan_read_in_order = 0` without the analyzer takes the legacy `ReadInOrderOptimizer`, which had
-- no placement check at all: it accepted a backward read for `ORDER BY negate(x)`, and even a forward one
-- for `ASC NULLS FIRST`.
SELECT 'the legacy read-in-order optimizer';
DROP TABLE IF EXISTS t_read_in_order_nan_legacy;
CREATE TABLE t_read_in_order_nan_legacy (x Float64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_read_in_order_nan_legacy VALUES (1), (2), (nan), (3);
SELECT negate(x) AS n FROM t_read_in_order_nan_legacy ORDER BY n
SETTINGS enable_analyzer = 0, query_plan_read_in_order = 0;
SELECT negate(x) AS n FROM t_read_in_order_nan_legacy ORDER BY n
SETTINGS enable_analyzer = 0, query_plan_read_in_order = 0, optimize_read_in_order = 0;
SELECT negate(x) AS n FROM t_read_in_order_nan_legacy ORDER BY n LIMIT 2
SETTINGS enable_analyzer = 0, query_plan_read_in_order = 0;
SELECT negate(x) AS n FROM t_read_in_order_nan_legacy ORDER BY n LIMIT 2
SETTINGS enable_analyzer = 0, query_plan_read_in_order = 0, optimize_read_in_order = 0;
SELECT x FROM t_read_in_order_nan_legacy ORDER BY x ASC NULLS FIRST
SETTINGS enable_analyzer = 0, query_plan_read_in_order = 0;
SELECT x FROM t_read_in_order_nan_legacy ORDER BY x ASC NULLS FIRST
SETTINGS enable_analyzer = 0, query_plan_read_in_order = 0, optimize_read_in_order = 0;
-- The orders a forward read does satisfy stay in order.
SELECT x FROM t_read_in_order_nan_legacy ORDER BY x
SETTINGS enable_analyzer = 0, query_plan_read_in_order = 0;
-- `enable_analyzer` cannot differ between a query and its subquery, so it is set for the session here.
SET enable_analyzer = 0;
SELECT count() > 0 AS read_in_order_used FROM (
    EXPLAIN SELECT x FROM t_read_in_order_nan_legacy ORDER BY x
    SETTINGS query_plan_read_in_order = 0, optimize_read_in_order = 1)
WHERE explain LIKE '%Prefix sort description%';
DROP TABLE t_read_in_order_nan_legacy;
