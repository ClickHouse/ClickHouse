-- https://github.com/ClickHouse/ClickHouse/issues/111077
-- A row policy and additional_table_filters referencing the same unprojected column
-- must both apply: previously the row-level filter (applied inside the reading step)
-- dropped the shared column from the block, so the additional filter failed with
-- NOT_FOUND_COLUMN_IN_BLOCK.

DROP TABLE IF EXISTS t_atf_rp;
DROP ROW POLICY IF EXISTS rp_atf ON t_atf_rp;

CREATE TABLE t_atf_rp (tenant String, k UInt32) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_atf_rp VALUES ('x', 1), ('y', 2), ('a', 3), ('b', 4);
CREATE ROW POLICY rp_atf ON t_atf_rp USING tenant != 'x' TO ALL;

-- Shared column `tenant` (policy) also referenced by additional_table_filters.
-- 'x' blocked by policy, 'y' blocked by the additional filter, 'a'/'b' pass.
SELECT count() FROM t_atf_rp WHERE k > 0
SETTINGS additional_table_filters = {'t_atf_rp': 'tenant != ''y'''};

SELECT tenant, k FROM t_atf_rp WHERE k > 0 ORDER BY k
SETTINGS additional_table_filters = {'t_atf_rp': 'tenant != ''y'''};

-- Not specific to automatic PREWHERE conversion.
SELECT count() FROM t_atf_rp WHERE k > 0
SETTINGS additional_table_filters = {'t_atf_rp': 'tenant != ''y'''}, optimize_move_to_prewhere = 0;

-- Additional filter mixing the shared policy column with another unprojected column.
DROP TABLE IF EXISTS t_atf_rp2;
DROP ROW POLICY IF EXISTS rp_atf2 ON t_atf_rp2;
CREATE TABLE t_atf_rp2 (tenant String, region String, k UInt32) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_atf_rp2 VALUES ('x', 'eu', 1), ('a', 'eu', 2), ('a', 'us', 3), ('y', 'eu', 4);
CREATE ROW POLICY rp_atf2 ON t_atf_rp2 USING tenant != 'x' TO ALL;
SELECT count() FROM t_atf_rp2 WHERE k > 0
SETTINGS additional_table_filters = {'t_atf_rp2': 'tenant != ''y'' AND region = ''eu'''};

-- Shared column under type wrappers: the row-level filter must still preserve it.
DROP TABLE IF EXISTS t_atf_rp_lc;
DROP ROW POLICY IF EXISTS rp_atf_lc ON t_atf_rp_lc;
CREATE TABLE t_atf_rp_lc (tenant LowCardinality(Nullable(String)), k UInt32) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_atf_rp_lc VALUES ('x', 1), ('y', 2), ('a', 3);
CREATE ROW POLICY rp_atf_lc ON t_atf_rp_lc USING tenant != 'x' TO ALL;
SELECT count() FROM t_atf_rp_lc WHERE k > 0
SETTINGS additional_table_filters = {'t_atf_rp_lc': 'tenant != ''y'''};

DROP ROW POLICY rp_atf ON t_atf_rp;
DROP ROW POLICY rp_atf2 ON t_atf_rp2;
DROP ROW POLICY rp_atf_lc ON t_atf_rp_lc;
DROP TABLE t_atf_rp;
DROP TABLE t_atf_rp2;
DROP TABLE t_atf_rp_lc;
