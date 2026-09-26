-- A not-matched row of an outer join carries the column default, which for `Date32` is 1970-01-01,
-- while the type default is 1900-01-01. The outer-to-inner and any-to-semi/anti conversions decide
-- whether the filter above the join rejects that row, so they must judge the value the join fills.
-- The cutoffs are fixed rather than now(): the type default wraps to 2036-02-07 under toDateTime,
-- so a now() predicate would stop detecting a reverted probe after that date.

SET query_plan_enable_optimizations = 1;                   -- the two conversion passes are gated on it
SET query_plan_convert_outer_join_to_inner_join = 1;       -- randomized off on ~5% of runs
SET query_plan_convert_any_join_to_semi_or_anti_join = 1;  -- randomized off on ~5% of runs
SET join_use_nulls = 0;                                    -- a not-matched row must carry the column
                                                           -- default, not NULL
SET enable_parallel_replicas = 0;                          -- the assertions below are about the local plan
SET allow_suspicious_low_cardinality_types = 1;            -- for the LowCardinality(Date32) fixture
SET session_timezone = 'UTC';                              -- in a timezone east of UTC, toDateTime(Date32
                                                           -- '1970-01-01') is below the epoch and wraps

DROP TABLE IF EXISTS kr_05233;
DROP TABLE IF EXISTS kl_05233;
DROP TABLE IF EXISTS kl_lc_05233;
DROP TABLE IF EXISTS kl_tup_05233;
DROP TABLE IF EXISTS kl_tup_mix_05233;
DROP TABLE IF EXISTS kie_l_05233;
DROP TABLE IF EXISTS kie_r_05233;
DROP TABLE IF EXISTS kl_date_05233;
DROP DICTIONARY IF EXISTS dict_05233;
DROP TABLE IF EXISTS kdr_05233;
DROP TABLE IF EXISTS dsrc_05233;

CREATE TABLE kr_05233 (k UInt32, v Int64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE kl_05233 (d Date32, v Int64) ENGINE = MergeTree ORDER BY d;
INSERT INTO kr_05233 VALUES (1, 2), (2, 3);   -- no v of kr matches kl, so both joined rows are not-matched
INSERT INTO kl_05233 VALUES ('2021-01-01', 1);

SELECT 'ground truth, conversion disabled', count()
FROM kr_05233 AS r LEFT JOIN kl_05233 AS l ON l.v = r.v
WHERE toDateTime(l.d) < toDateTime('2020-07-25 12:00:00')
SETTINGS query_plan_convert_outer_join_to_inner_join = 0;

SELECT 'LEFT JOIN, toDateTime', count()
FROM kr_05233 AS r LEFT JOIN kl_05233 AS l ON l.v = r.v
WHERE toDateTime(l.d) < toDateTime('2020-07-25 12:00:00');

SELECT 'RIGHT JOIN, toDate', count()
FROM kl_05233 AS l RIGHT JOIN kr_05233 AS r ON l.v = r.v
WHERE toDate(l.d) <= toDate('2020-07-25');

SELECT 'FULL JOIN, toDateTime', count()
FROM kl_05233 AS l FULL JOIN kr_05233 AS r ON l.v = r.v
WHERE toDateTime(l.d) <= toDateTime('2020-07-25 12:00:00');

SELECT 'ANY LEFT JOIN, toDateTime', count()
FROM kr_05233 AS r ANY LEFT JOIN kl_05233 AS l ON l.v = r.v
WHERE toDateTime(l.d) < toDateTime('2020-07-25 12:00:00');

-- No conversion function at all: 1970-01-01 passes this predicate and 1900-01-01 does not.
SELECT 'LEFT JOIN, no wrapper', count()
FROM kr_05233 AS r LEFT JOIN kl_05233 AS l ON l.v = r.v
WHERE l.d > toDate32('1950-01-01');

-- 1970-01-01 fails this predicate, so converting to INNER is correct and must still happen.
SELECT 'conversion preserved, rows', count()
FROM kr_05233 AS r LEFT JOIN kl_05233 AS l ON l.v = r.v
WHERE l.d > toDate32('1980-01-01');

SELECT 'conversion preserved, plan', count() FROM (
    EXPLAIN actions = 1
    SELECT count() FROM kr_05233 AS r LEFT JOIN kl_05233 AS l ON l.v = r.v
    WHERE l.d > toDate32('1980-01-01')
) WHERE explain ILIKE '%Type: inner%';

CREATE TABLE kl_lc_05233 (d LowCardinality(Date32), v Int64) ENGINE = MergeTree ORDER BY v;
INSERT INTO kl_lc_05233 VALUES ('2021-01-01', 1);

SELECT 'LowCardinality(Date32)', count()
FROM kr_05233 AS r LEFT JOIN kl_lc_05233 AS l ON l.v = r.v
WHERE toDateTime(l.d) < toDateTime('2020-07-25 12:00:00');

SELECT 'LowCardinality(Date32), conversion preserved, plan', count() FROM (
    EXPLAIN actions = 1
    SELECT count() FROM kr_05233 AS r LEFT JOIN kl_lc_05233 AS l ON l.v = r.v
    WHERE l.d > toDate32('1980-01-01')
) WHERE explain ILIKE '%Type: inner%';

CREATE TABLE kl_tup_05233 (t Tuple(Date32), v Int64) ENGINE = MergeTree ORDER BY v;
INSERT INTO kl_tup_05233 VALUES (tuple(toDate32('2021-01-01')), 1);

SELECT 'Tuple(Date32)', count()
FROM kr_05233 AS r LEFT JOIN kl_tup_05233 AS l ON l.v = r.v
WHERE toDateTime(l.t.1) < toDateTime('2020-07-25 12:00:00');

SELECT 'Tuple(Date32), conversion preserved, plan', count() FROM (
    EXPLAIN actions = 1
    SELECT count() FROM kr_05233 AS r LEFT JOIN kl_tup_05233 AS l ON l.v = r.v
    WHERE toDateTime(materialize(l.t).1) > toDateTime('2030-01-01')
) WHERE explain ILIKE '%Type: inner%';

CREATE TABLE kl_tup_mix_05233 (t Tuple(Date32, Enum8('x' = 1)), v Int64) ENGINE = MergeTree ORDER BY v;
INSERT INTO kl_tup_mix_05233 VALUES (tuple(toDate32('2021-01-01'), 'x'), 1);

-- A mixed composite: both join fill families agree on the `Date32` element and differ only on the
-- `Enum8` one, so the whole tuple's type default must not stand in for the element read here.
SELECT 'Tuple(Date32, Enum8), whole tuple in the filter', count()
FROM kr_05233 AS r LEFT JOIN kl_tup_mix_05233 AS l ON l.v = r.v
WHERE toDateTime(materialize(l.t).1) < toDateTime('2020-07-25 12:00:00');

SELECT 'Tuple(Date32, Enum8), subcolumn extraction off', count()
FROM kr_05233 AS r LEFT JOIN kl_tup_mix_05233 AS l ON l.v = r.v
WHERE toDateTime(l.t.1) < toDateTime('2020-07-25 12:00:00')
SETTINGS optimize_functions_to_subcolumns = 0;

-- An inequality-only ON routes to `ie_join`, which pads a not-matched row through
-- `IColumn::insertDefault` (0) while the hash join pads through `DataTypeEnum::insertDefaultInto`
-- (the first declared value). The old probe used the latter, so it judged a filter the padded row
-- passes.
CREATE TABLE kie_l_05233 (x UInt8, y UInt8) ENGINE = MergeTree ORDER BY x;
CREATE TABLE kie_r_05233 (x UInt8, y UInt8, e Enum8('a' = 1)) ENGINE = MergeTree ORDER BY x;
INSERT INTO kie_l_05233 VALUES (1, 1), (2, 2);
INSERT INTO kie_r_05233 VALUES (0, 0, 'a');   -- no row of kie_r can match on both inequalities

SELECT 'Enum8 over ie_join, conversion disabled', count()
FROM kie_l_05233 AS l LEFT JOIN kie_r_05233 AS r ON l.x > r.x AND l.y < r.y
WHERE toInt8(r.e) = 0
SETTINGS join_algorithm = 'ie_join', query_plan_convert_outer_join_to_inner_join = 0;

SELECT 'Enum8 over ie_join', count()
FROM kie_l_05233 AS l LEFT JOIN kie_r_05233 AS r ON l.x > r.x AND l.y < r.y
WHERE toInt8(r.e) = 0
SETTINGS join_algorithm = 'ie_join';

-- `Date` agrees on both values, so neither its result nor its conversion may move.
CREATE TABLE kl_date_05233 (d Date, v Int64) ENGINE = MergeTree ORDER BY d;
INSERT INTO kl_date_05233 VALUES ('2021-01-01', 1);

SELECT 'Date is unaffected, rows', count()
FROM kr_05233 AS r LEFT JOIN kl_date_05233 AS l ON l.v = r.v
WHERE toDateTime(l.d) < toDateTime('2020-07-25 12:00:00');

SELECT 'Date is unaffected, plan', count() FROM (
    EXPLAIN actions = 1
    SELECT count() FROM kr_05233 AS r LEFT JOIN kl_date_05233 AS l ON l.v = r.v
    WHERE l.d > toDate('1980-01-01')
) WHERE explain ILIKE '%Type: inner%';

-- A prepared key-value lookup fills a missing key with the type default, so no probe value models
-- every algorithm the second pass may pick. The conversions must not change the answer there; the
-- answer itself is algorithm-dependent and is deliberately not asserted.
CREATE TABLE dsrc_05233 (k UInt64, d Date32) ENGINE = MergeTree ORDER BY k;
INSERT INTO dsrc_05233 VALUES (1, '2021-01-01');
CREATE TABLE kdr_05233 (k UInt32, v UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO kdr_05233 VALUES (1, 7), (2, 8);   -- neither key is in the dictionary
CREATE DICTIONARY dict_05233 (k UInt64, d Date32) PRIMARY KEY k
SOURCE(CLICKHOUSE(TABLE 'dsrc_05233')) LAYOUT(HASHED()) LIFETIME(MIN 0 MAX 0);

SELECT 'dictionary LEFT JOIN, answer unchanged',
    (SELECT count() FROM kdr_05233 AS r LEFT JOIN dict_05233 AS d ON d.k = r.v
     WHERE toDateTime(d.d) > toDateTime('2030-01-01'))
  = (SELECT count() FROM kdr_05233 AS r LEFT JOIN dict_05233 AS d ON d.k = r.v
     WHERE toDateTime(d.d) > toDateTime('2030-01-01')
     SETTINGS query_plan_convert_outer_join_to_inner_join = 0);

SELECT 'dictionary ANY LEFT JOIN, answer unchanged',
    (SELECT count() FROM kdr_05233 AS r ANY LEFT JOIN dict_05233 AS d ON d.k = r.v
     WHERE toDateTime(d.d) > toDateTime('2030-01-01')
     SETTINGS join_algorithm = 'direct')
  = (SELECT count() FROM kdr_05233 AS r ANY LEFT JOIN dict_05233 AS d ON d.k = r.v
     WHERE toDateTime(d.d) > toDateTime('2030-01-01')
     SETTINGS join_algorithm = 'direct',
              query_plan_convert_any_join_to_semi_or_anti_join = 0,
              query_plan_convert_outer_join_to_inner_join = 0);

SELECT 'dictionary direct join, answer unchanged',
    (SELECT count() FROM kdr_05233 AS r LEFT JOIN dict_05233 AS d ON d.k = r.v
     WHERE toDateTime(d.d) > toDateTime('2030-01-01')
     SETTINGS join_algorithm = 'direct')
  = (SELECT count() FROM kdr_05233 AS r LEFT JOIN dict_05233 AS d ON d.k = r.v
     WHERE toDateTime(d.d) > toDateTime('2030-01-01')
     SETTINGS join_algorithm = 'direct', query_plan_convert_outer_join_to_inner_join = 0);

DROP DICTIONARY dict_05233;
DROP TABLE kdr_05233;
DROP TABLE dsrc_05233;
DROP TABLE kl_date_05233;
DROP TABLE kie_r_05233;
DROP TABLE kie_l_05233;
DROP TABLE kl_tup_mix_05233;
DROP TABLE kl_tup_05233;
DROP TABLE kl_lc_05233;
DROP TABLE kl_05233;
DROP TABLE kr_05233;
