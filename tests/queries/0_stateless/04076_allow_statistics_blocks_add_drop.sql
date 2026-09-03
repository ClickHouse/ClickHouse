-- Verify that allow_statistics=0 blocks ADD, DROP, and MATERIALIZE STATISTICS.
SET allow_statistics = 1;

DROP TABLE IF EXISTS t_allow_statistics_check;
CREATE TABLE t_allow_statistics_check (x UInt32) ENGINE = MergeTree ORDER BY x;

SET allow_statistics = 0;

ALTER TABLE t_allow_statistics_check ADD STATISTICS x TYPE tdigest; -- { serverError INCORRECT_QUERY }
ALTER TABLE t_allow_statistics_check MODIFY STATISTICS x TYPE tdigest; -- { serverError INCORRECT_QUERY }

SET allow_statistics = 1;
ALTER TABLE t_allow_statistics_check ADD STATISTICS x TYPE tdigest;
SET allow_statistics = 0;

ALTER TABLE t_allow_statistics_check DROP STATISTICS x; -- { serverError INCORRECT_QUERY }
ALTER TABLE t_allow_statistics_check MATERIALIZE STATISTICS x; -- { serverError INCORRECT_QUERY }

DROP TABLE IF EXISTS t_allow_statistics_check;
