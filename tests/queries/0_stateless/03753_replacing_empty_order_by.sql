DROP TABLE IF EXISTS t_empty_order_key;

SET allow_suspicious_primary_key = 0;

-- CREATE TABLE t_empty_order_key(c0 String, c1 String) ENGINE = ReplacingMergeTree() ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }

SET allow_suspicious_primary_key = 1;

CREATE TABLE t_empty_order_key(c0 String, c1 String) ENGINE = ReplacingMergeTree() ORDER BY tuple();

INSERT INTO TABLE t_empty_order_key (c0, c1) VALUES ('foo', 'bar');
OPTIMIZE TABLE t_empty_order_key FINAL;
SELECT * FROM t_empty_order_key ORDER BY c0;
DROP TABLE t_empty_order_key;

-- Check with forced vertical merge
CREATE TABLE t_empty_order_key(c0 String, c1 String) ENGINE = ReplacingMergeTree() ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, vertical_merge_algorithm_min_bytes_to_activate = 1, vertical_merge_algorithm_min_rows_to_activate = 0, index_granularity = 1, vertical_merge_algorithm_min_columns_to_activate = 1;

INSERT INTO TABLE t_empty_order_key (c0, c1) VALUES ('foo', 'bar');
OPTIMIZE TABLE t_empty_order_key FINAL;
SELECT * FROM t_empty_order_key ORDER BY c0;
DROP TABLE t_empty_order_key;
