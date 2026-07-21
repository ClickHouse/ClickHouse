-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/106419
--
-- A `WHERE` filter of the form `toStartOfYear(<Date32 column>) < toStartOfYear(<Date constant>)`
-- returned 0 rows when the `Date32` column contained a few pre-1970 values inside a merge-formed
-- part: the pre-epoch values overflowed into far-future dates and poisoned the part's value range
-- used by the monotonic-function filter analysis, so the whole part was wrongly skipped.
-- `WHERE expr` must return the same rows that `countIf(expr)` counts.

DROP TABLE IF EXISTS issue_repro;
CREATE TABLE issue_repro (c1 Date32) ENGINE = MergeTree ORDER BY tuple();

-- 9991 in-range rows (1971..1998) then 9 pre-1970 outliers, as two separate parts
INSERT INTO issue_repro SELECT toDate32('1971-01-01') + toIntervalDay(number % 18000) FROM numbers(9991);
INSERT INTO issue_repro SELECT toDate32('1905-01-01') + toIntervalDay(number * 30)    FROM numbers(9);

-- merge into a single part (also happens via background merge)
OPTIMIZE TABLE issue_repro FINAL;

-- the predicate is true for all rows: pre-epoch results clamp to 1970-01-01
SELECT countIf(toStartOfYear(c1) < toStartOfYear(toDate('2021-06-15'))) FROM issue_repro;
SELECT count() FROM issue_repro WHERE toStartOfYear(c1) < toStartOfYear(toDate('2021-06-15'));

SELECT countIf(toStartOfMonth(c1) < toStartOfMonth(toDate('2021-06-15'))) FROM issue_repro;
SELECT count() FROM issue_repro WHERE toStartOfMonth(c1) < toStartOfMonth(toDate('2021-06-15'));

SELECT countIf(toStartOfQuarter(c1) < toStartOfQuarter(toDate('2021-06-15'))) FROM issue_repro;
SELECT count() FROM issue_repro WHERE toStartOfQuarter(c1) < toStartOfQuarter(toDate('2021-06-15'));

-- only the 9 pre-epoch outliers clamp to exactly 1970-01-01
SELECT countIf(toStartOfYear(c1) = toDate('1970-01-01')) FROM issue_repro;
SELECT count() FROM issue_repro WHERE toStartOfYear(c1) = toDate('1970-01-01');

DROP TABLE issue_repro;
