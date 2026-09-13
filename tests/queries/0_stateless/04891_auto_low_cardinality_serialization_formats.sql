-- Verify that output formats can concatenate chunks from parts with default and automatic LowCardinality serialization.
SET allow_experimental_statistics = 1;
SET materialize_statistics_on_insert = 1;

DROP TABLE IF EXISTS t_auto_lc_formats;

CREATE TABLE t_auto_lc_formats
(
    id UInt64,
    value String STATISTICS(uniq)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS max_uniq_number_for_low_cardinality = 1000;

SYSTEM STOP MERGES t_auto_lc_formats;

INSERT INTO t_auto_lc_formats VALUES (1, 'encoded');
ALTER TABLE t_auto_lc_formats MODIFY SETTING max_uniq_number_for_low_cardinality = 0;
INSERT INTO t_auto_lc_formats VALUES (2, 'plain');

SELECT value FROM t_auto_lc_formats ORDER BY id SETTINGS max_block_size = 1, output_format_json_pretty_print = 0 FORMAT JSONColumns;

DROP TABLE t_auto_lc_formats;
