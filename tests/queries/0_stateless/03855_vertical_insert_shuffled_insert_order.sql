-- Test: shuffled insert order still yields sorted data on read.
DROP TABLE IF EXISTS t_vi_shuffled;

CREATE TABLE t_vi_shuffled
(
    k UInt64,
    id UInt64,
    payload String
)
ENGINE = MergeTree
ORDER BY (k, id)
SETTINGS
    min_rows_for_wide_part = 0,
    min_bytes_for_wide_part = 0,
    enable_vertical_insert_algorithm = 1,
    vertical_insert_algorithm_min_rows_to_activate = 1,
    vertical_insert_algorithm_min_bytes_to_activate = 0,
    vertical_insert_algorithm_min_columns_to_activate = 1;

INSERT INTO t_vi_shuffled VALUES
    (2, 5, '2-5'),
    (1, 3, '1-3'),
    (3, 1, '3-1'),
    (1, 2, '1-2'),
    (2, 1, '2-1'),
    (3, 0, '3-0'),
    (1, 1, '1-1'),
    (2, 0, '2-0'),
    (3, 2, '3-2'),
    (2, 2, '2-2');

SELECT countIf((k, id) < lag) FROM
(
    SELECT
        k,
        id,
        lagInFrame((k, id), 1, (k, id)) OVER (ORDER BY _part, _part_offset) AS lag
    FROM t_vi_shuffled
);

DROP TABLE t_vi_shuffled;
