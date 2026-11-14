-- Test that add_minmax_index_for_numeric_columns is enabled by default

DROP TABLE IF EXISTS t_minmax_default;

CREATE TABLE t_minmax_default (x UInt64, y Float64, s String) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_minmax_default VALUES (1, 0.5, 'a'), (2, 1.5, 'b'), (3, 2.5, 'c');

-- Numeric columns should have auto minmax indices, so this query should skip granules
SELECT count() FROM t_minmax_default WHERE x > 100 SETTINGS force_data_skipping_indices = 'auto_minmax_index_x';

-- Test that opt-out works
DROP TABLE IF EXISTS t_minmax_disabled;

CREATE TABLE t_minmax_disabled (x UInt64, y Float64, s String) ENGINE = MergeTree ORDER BY tuple() SETTINGS add_minmax_index_for_numeric_columns = 0;

INSERT INTO t_minmax_disabled VALUES (1, 0.5, 'a'), (2, 1.5, 'b'), (3, 2.5, 'c');

-- Should fail because there is no auto minmax index when the setting is disabled
SELECT count() FROM t_minmax_disabled WHERE x > 100 SETTINGS force_data_skipping_indices = 'auto_minmax_index_x'; -- { serverError INDEX_NOT_USED }

DROP TABLE t_minmax_default;
DROP TABLE t_minmax_disabled;
