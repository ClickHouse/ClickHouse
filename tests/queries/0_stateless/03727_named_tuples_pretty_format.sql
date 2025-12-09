SELECT
    1 AS top_int,
    NULL AS top_null,
    'Row1' AS top_str,
    CAST((
        42,                                     
        'ClickHouse',                             
        CAST((1, 'nested', NULL) AS Tuple(n_id UInt8, n_name String, n_null Nullable(Int32))),
        CAST([(100, 'foo'), (200, 'bar')] AS Array(Tuple(a UInt16, b String))),
        NULL,                                     
        999                                       
    ) AS Tuple(
        id UInt64,
        name String,
        nested Tuple(n_id UInt8, n_name String, n_null Nullable(Int32)),
        arr Array(Tuple(a UInt16, b String)),
        optional_field Nullable(String),
        extra_id UInt32
    )) AS my_named_tuple
UNION ALL
SELECT
    2 AS top_int,
    'Hello' AS top_null,
    NULL AS top_str,
    CAST((
        7, 
        'Database',
        CAST((2, 'inner', 123) AS Tuple(n_id UInt8, n_name String, n_null Nullable(Int32))), 
        CAST([(300, 'baz'), (400, 'qux')] AS Array(Tuple(a UInt16, b String))),
        'Some text',                              
        555                                       
    ) AS Tuple(
        id UInt64,
        name String,
        nested Tuple(n_id UInt8, n_name String, n_null Nullable(Int32)),
        arr Array(Tuple(a UInt16, b String)),
        optional_field Nullable(String),
        extra_id UInt32
    )) AS my_named_tuple 
FORMAT PRETTY
