DROP TABLE IF EXISTS t_fill_arrays;

CREATE TABLE t_fill_arrays
(
    `id` String,
    `mapCol` Map(String, Array(String)),
)
ENGINE = MergeTree
ORDER BY id
SETTINGS vertical_merge_algorithm_min_rows_to_activate = 1, vertical_merge_algorithm_min_columns_to_activate = 1, min_bytes_for_full_part_storage = 0;

INSERT INTO t_fill_arrays (id) SELECT hex(number) FROM numbers(10000);

ALTER TABLE t_fill_arrays ADD COLUMN arrCol Array(String) DEFAULT [];

INSERT INTO t_fill_arrays (id) SELECT hex(number) FROM numbers(10000);

SELECT count() FROM t_fill_arrays WHERE NOT ignore(arrCol, mapCol.values);

OPTIMIZE TABLE t_fill_arrays FINAL;

DROP TABLE t_fill_arrays;
