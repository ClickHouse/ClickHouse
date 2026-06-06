SET allow_experimental_nullable_array_type = 1;

DROP TABLE IF EXISTS nullable_array_size0_subcolumn;

CREATE TABLE nullable_array_size0_subcolumn
(
    id UInt8,
    a Nullable(Array(Int32))
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO nullable_array_size0_subcolumn VALUES (1, NULL), (2, []), (3, [1, 2]);

SELECT throwIf(toTypeName(a.size0) != 'Nullable(UInt64)')
FROM nullable_array_size0_subcolumn
LIMIT 1;

SELECT throwIf(groupArray(ifNull(size, 999)) != [999, 0, 2])
FROM
(
    SELECT a.size0 AS size
    FROM nullable_array_size0_subcolumn
    ORDER BY id
);

DROP TABLE nullable_array_size0_subcolumn;
