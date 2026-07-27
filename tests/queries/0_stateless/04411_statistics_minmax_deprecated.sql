DROP TABLE IF EXISTS t_minmax_deprecated;

-- Explicit `minmax` in CREATE is rejected.
CREATE TABLE t_minmax_deprecated (a UInt64 STATISTICS(minmax)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError INCORRECT_QUERY }

-- `minmax` via the auto statistics setting is rejected too, regardless of its position in the list.
CREATE TABLE t_minmax_deprecated (a UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS auto_statistics_types = 'minmax'; -- { serverError INCORRECT_QUERY }
CREATE TABLE t_minmax_deprecated (a UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS auto_statistics_types = 'basic, minmax, uniq'; -- { serverError INCORRECT_QUERY }

-- A non-`minmax` auto statistics setting is accepted.
CREATE TABLE t_minmax_deprecated (a UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS auto_statistics_types = 'basic, uniq';
DROP TABLE t_minmax_deprecated;

-- `basic` (the replacement) works.
CREATE TABLE t_minmax_deprecated (a UInt64 STATISTICS(basic)) ENGINE = MergeTree ORDER BY tuple();

-- Adding / modifying `minmax` via ALTER is rejected, while `basic` is accepted.
ALTER TABLE t_minmax_deprecated ADD STATISTICS a TYPE minmax; -- { serverError INCORRECT_QUERY }
ALTER TABLE t_minmax_deprecated MODIFY STATISTICS a TYPE minmax; -- { serverError INCORRECT_QUERY }
ALTER TABLE t_minmax_deprecated MODIFY SETTING auto_statistics_types = 'minmax, uniq'; -- { serverError INCORRECT_QUERY }
ALTER TABLE t_minmax_deprecated MODIFY STATISTICS a TYPE basic;

SHOW CREATE TABLE t_minmax_deprecated;

-- Migrating the auto statistics setting to a non-`minmax` value is allowed.
ALTER TABLE t_minmax_deprecated MODIFY SETTING auto_statistics_types = 'basic, uniq';

DROP TABLE t_minmax_deprecated;
