DROP TABLE IF EXISTS 04060_test;

CREATE TABLE 04060_test
(
    key            String,
    str_col        String,
    arr_col        Array(String),
    tuple_col      Tuple(x String, y UInt32),
    map_col        Map(String, UInt32),
    update_column  Nullable(String)
)
ENGINE = CoalescingMergeTree(update_column)
ORDER BY key;

INSERT INTO 04060_test (key, str_col, arr_col, tuple_col, map_col)
VALUES ('k', 'first', ['a', 'b'], ('hello', 42), {'x': 1, 'y': 2});

INSERT INTO 04060_test (key, update_column)
VALUES ('k', 'v2');

SELECT str_col, arr_col, tuple_col, map_col, update_column
FROM 04060_test FINAL;

OPTIMIZE TABLE 04060_test FINAL;

SELECT str_col, arr_col, tuple_col, map_col, update_column
FROM 04060_test;

DROP TABLE 04060_test;
