SET output_format_pretty_display_footer_column_names=0;
SET output_format_pretty_color=1;
SET read_in_order_two_level_merge_threshold=1000000;

DROP TABLE IF EXISTS t;
CREATE TABLE t(a UInt64)
ENGINE = MergeTree
ORDER BY a;

INSERT INTO t SELECT * FROM numbers_mt(1e3);
OPTIMIZE TABLE t FINAL;

EXPLAIN PIPELINE
SELECT a
FROM t
GROUP BY a
FORMAT PrettySpace
SETTINGS optimize_aggregation_in_order = 1;

DROP TABLE t;
