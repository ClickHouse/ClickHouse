DROP TABLE IF EXISTS t_proj_external;

CREATE TABLE t_proj_external
(
    k1 UInt32,
    k2 UInt32,
    k3 UInt32,
    value UInt32
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO t_proj_external SELECT 1, number%2, number%4, number FROM numbers(50000);

SYSTEM STOP MERGES t_proj_external;

ALTER TABLE t_proj_external ADD PROJECTION aaaa (
    SELECT
        k1,
        k2,
        k3,
        sum(value)
    GROUP BY k1, k2, k3
);

INSERT INTO t_proj_external SELECT 1, number%2, number%4, number FROM numbers(100000) LIMIT 50000, 100000;

SELECT '*** correct aggregation ***';

SELECT k1, k2, k3, sum(value) v FROM t_proj_external GROUP BY k1, k2, k3 ORDER BY k1, k2, k3 SETTINGS optimize_use_projections = 0;

SELECT '*** correct aggregation with projection ***';

SELECT k1, k2, k3, sum(value) v FROM t_proj_external GROUP BY k1, k2, k3 ORDER BY k1, k2, k3;

SELECT '*** optimize_aggregation_in_order = 0, max_bytes_before_external_group_by = 1, group_by_two_level_threshold = 1 ***';

SELECT k1, k2, k3, sum(value) v FROM t_proj_external GROUP BY k1, k2, k3 ORDER BY k1, k2, k3 SETTINGS optimize_aggregation_in_order = 0, max_bytes_before_external_group_by = 1, group_by_two_level_threshold = 1;

SELECT '*** optimize_aggregation_in_order = 1, max_bytes_before_external_group_by = 1, group_by_two_level_threshold = 1 ***';

SELECT k1, k2, k3, sum(value) v FROM t_proj_external GROUP BY k1, k2, k3 ORDER BY k1, k2, k3 SETTINGS optimize_aggregation_in_order = 1, max_bytes_before_external_group_by = 1, group_by_two_level_threshold = 1;

SYSTEM START MERGES t_proj_external;

ALTER TABLE t_proj_external MATERIALIZE PROJECTION aaaa SETTINGS mutations_sync = 2;

SELECT '*** after materialization ***';

SELECT '*** correct aggregation ***';

SELECT k1, k2, k3, sum(value) v FROM t_proj_external GROUP BY k1, k2, k3 ORDER BY k1, k2, k3 SETTINGS optimize_use_projections = 0;

SELECT '*** correct aggregation with projection ***';

SELECT k1, k2, k3, sum(value) v FROM t_proj_external GROUP BY k1, k2, k3 ORDER BY k1, k2, k3;

SELECT '*** optimize_aggregation_in_order = 0, max_bytes_before_external_group_by = 1, group_by_two_level_threshold = 1 ***';

SELECT k1, k2, k3, sum(value) v FROM t_proj_external GROUP BY k1, k2, k3 ORDER BY k1, k2, k3 SETTINGS optimize_aggregation_in_order = 0, max_bytes_before_external_group_by = 1, group_by_two_level_threshold = 1;

SELECT '*** optimize_aggregation_in_order = 1, max_bytes_before_external_group_by = 1, group_by_two_level_threshold = 1 ***';

SELECT k1, k2, k3, sum(value) v FROM t_proj_external GROUP BY k1, k2, k3 ORDER BY k1, k2, k3 SETTINGS optimize_aggregation_in_order = 1, max_bytes_before_external_group_by = 1, group_by_two_level_threshold = 1;

DROP TABLE IF EXISTS t_proj_external;
