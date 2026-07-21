-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/111143
-- A single-column wide MergeTree whose only column has a fully-expiring column TTL used to
-- abort a merge with "Cannot calculate columns sizes when columns or checksums are not
-- initialized": the merge dropped the sole column, leaving a zero-column part. A part must
-- keep at least one physical column, so one is now retained (holding default values).

SET allow_suspicious_ttl_expressions = 1;

-- { echoOn }

-- No DEFAULT: the retained column reads as the type default after the TTL fully expires.
DROP TABLE IF EXISTS t_ttl_single_no_default;
CREATE TABLE t_ttl_single_no_default (c0 Int32 TTL now() - INTERVAL 1 DAY)
ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 1;
INSERT INTO t_ttl_single_no_default VALUES (1), (2), (3);
OPTIMIZE TABLE t_ttl_single_no_default FINAL;
SELECT count(), groupArray(c0) FROM t_ttl_single_no_default;
DETACH TABLE t_ttl_single_no_default;
ATTACH TABLE t_ttl_single_no_default;
SELECT count() FROM t_ttl_single_no_default;
DROP TABLE t_ttl_single_no_default;

-- With DEFAULT: the retained column reads as its DDL default expression.
DROP TABLE IF EXISTS t_ttl_single_default;
CREATE TABLE t_ttl_single_default (c0 Int32 DEFAULT 42 TTL now() - INTERVAL 1 DAY)
ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 1;
INSERT INTO t_ttl_single_default VALUES (1), (2), (3);
OPTIMIZE TABLE t_ttl_single_default FINAL;
SELECT count(), groupArray(c0) FROM t_ttl_single_default;
DROP TABLE t_ttl_single_default;

-- Every column of a multi-column table expires: one column is still retained, no crash.
DROP TABLE IF EXISTS t_ttl_all_expired;
CREATE TABLE t_ttl_all_expired (a Int32 TTL now() - INTERVAL 1 DAY, b String TTL now() - INTERVAL 1 DAY)
ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 1;
INSERT INTO t_ttl_all_expired VALUES (1, 'x'), (2, 'y');
OPTIMIZE TABLE t_ttl_all_expired FINAL;
SELECT count(), groupArray(a), groupArray(b) FROM t_ttl_all_expired;
DROP TABLE t_ttl_all_expired;

-- { echoOff }
