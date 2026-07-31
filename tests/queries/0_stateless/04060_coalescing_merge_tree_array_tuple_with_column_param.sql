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

-- Wrap `map_col` in `mapSort` so the output is deterministic regardless of
-- the on-disk map serialization version (`map_serialization_version_for_zero_level_parts`),
-- which CI randomizes (`with_buckets` reorders keys by hash bucket).
SELECT str_col, arr_col, tuple_col, mapSort(map_col), update_column
FROM 04060_test FINAL;

OPTIMIZE TABLE 04060_test FINAL;

SELECT str_col, arr_col, tuple_col, mapSort(map_col), update_column
FROM 04060_test;

DROP TABLE 04060_test;
