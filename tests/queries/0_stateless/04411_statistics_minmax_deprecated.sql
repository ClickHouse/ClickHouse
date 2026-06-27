-- Tags: no-fasttest
-- Tests that the deprecated `minmax` column statistics type can no longer be created, while `basic`
-- (its superset) works. Existing tables/parts that still reference `minmax` are covered by the
-- integration test test_statistics_minmax_upgrade.

SET allow_statistics = 1;

DROP TABLE IF EXISTS t_minmax_deprecated;

-- Explicit `minmax` in CREATE is rejected.
CREATE TABLE t_minmax_deprecated (a UInt64 STATISTICS(minmax)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError INCORRECT_QUERY }

-- `minmax` via auto statistics is rejected too.
CREATE TABLE t_minmax_deprecated (a UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS auto_statistics_types = 'minmax'; -- { serverError INCORRECT_QUERY }

-- `basic` (the replacement) works.
CREATE TABLE t_minmax_deprecated (a UInt64 STATISTICS(basic)) ENGINE = MergeTree ORDER BY tuple();

-- Adding / modifying `minmax` via ALTER is rejected, while `basic` is accepted.
ALTER TABLE t_minmax_deprecated ADD STATISTICS a TYPE minmax; -- { serverError INCORRECT_QUERY }
ALTER TABLE t_minmax_deprecated MODIFY STATISTICS a TYPE minmax; -- { serverError INCORRECT_QUERY }
ALTER TABLE t_minmax_deprecated MODIFY SETTING auto_statistics_types = 'minmax, uniq'; -- { serverError INCORRECT_QUERY }
ALTER TABLE t_minmax_deprecated MODIFY STATISTICS a TYPE basic;

SHOW CREATE TABLE t_minmax_deprecated;

DROP TABLE t_minmax_deprecated;
