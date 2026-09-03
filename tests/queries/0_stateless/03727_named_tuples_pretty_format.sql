WITH data AS (
    SELECT
        number AS row_num,
        [1, 2] AS top_ints,
        [NULL, 'Hello'] AS top_nulls,
        ['Row1', NULL] AS top_strs,
        [42, 7] AS ids,
        ['ClickHouse', 'Database'] AS names,
        [(1, 'nested', NULL), (2, 'inner', 123)] AS nesteds,
        [
            [(100, 'foo'), (200, 'bar')],
            [(300, 'baz'), (400, 'qux')]
        ] AS arrs,
        [NULL, 'Some text'] AS optionals,
        [999, 555] AS extra_ids
    FROM numbers(1, 2)
)
SELECT
    top_ints[row_num] AS top_int,
    top_nulls[row_num] AS top_null,
    top_strs[row_num] AS top_str,
    (
        ids[row_num],
        names[row_num],
        nesteds[row_num],
        arrs[row_num],
        optionals[row_num],
        extra_ids[row_num]
    )::Tuple(
        id UInt64,
        name String,
        nested Tuple(n_id UInt8, n_name String, n_null Nullable(Int32)),
        arr Array(Tuple(a UInt16, b String)),
        optional_field Nullable(String),
        extra_id UInt32
    ) AS my_named_tuple
FROM data
ORDER BY top_int
FORMAT PrettyMonoBlock;
