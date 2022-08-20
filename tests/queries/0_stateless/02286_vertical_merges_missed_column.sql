DROP TABLE IF EXISTS t_vertical_merges;

CREATE TABLE t_vertical_merges
(
  a   Nullable(String),
  b   Int8
)
ENGINE = MergeTree
ORDER BY tuple()
settings
    vertical_merge_algorithm_min_columns_to_activate=1,
    vertical_merge_algorithm_min_rows_to_activate=1,
    min_bytes_for_wide_part=0;

INSERT INTO t_vertical_merges SELECT NULL, 1;
ALTER TABLE t_vertical_merges ADD COLUMN c String;
OPTIMIZE TABLE t_vertical_merges FINAL;
SELECT a, b, c FROM t_vertical_merges;

DROP TABLE IF EXISTS t_vertical_merges;

CREATE TABLE t_vertical_merges
(
  a   Array(Int16),
  b   Int8
)
ENGINE = MergeTree
ORDER BY tuple()
settings
    vertical_merge_algorithm_min_columns_to_activate=1,
    vertical_merge_algorithm_min_rows_to_activate=1,
    min_bytes_for_wide_part=0;

INSERT INTO t_vertical_merges SELECT [], 1;
ALTER TABLE t_vertical_merges CLEAR COLUMN b;
OPTIMIZE TABLE t_vertical_merges FINAL;
SELECT a, b FROM t_vertical_merges;

DROP TABLE IF EXISTS t_vertical_merges;
