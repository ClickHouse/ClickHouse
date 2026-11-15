-- Regression for the case when the JOIN contains const and non-const blocks, which leads to UB:
--
--   Too large size (18446603496615682040) passed to allocator. It indicates an error
WITH
    input_1 AS (SELECT number::String AS parent_id, number::String as id, number::String as value FROM numbers_mt(1e6)),
    dimensions_1 AS (SELECT number::String AS value_id FROM numbers_mt(1e6)),
    dimensions_2 AS (SELECT number::String AS value_id FROM numbers_mt(1e6)),
    parents AS
    (
        SELECT 'foo' AS type, parent_id
        FROM input_1
        GROUP BY parent_id
    ),
    parents_with_value AS
    (
        SELECT type, parent_id, t.value
        FROM parents
        LEFT JOIN input_1 AS t ON t.id = parents.parent_id
    ),
    values AS
    (
        SELECT 'foo' AS type, '' AS parent_id, value
        FROM input_1
    ),
    all AS
    (
        SELECT * FROM parents_with_value
        UNION ALL
        SELECT * FROM values
    )
SELECT type, value
FROM all
INNER JOIN dimensions_1 AS dim1 ON all.value = dim1.value_id
INNER JOIN dimensions_2 AS dim2 ON all.value = dim2.value_id
FORMAT `Null`
SETTINGS max_block_size=65535, max_joined_block_size_rows=65535, max_threads=32;
