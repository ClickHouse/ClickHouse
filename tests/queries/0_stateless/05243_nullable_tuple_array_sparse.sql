DROP TABLE IF EXISTS nullable_tuple_array_sparse;

CREATE TABLE nullable_tuple_array_sparse
(
    t Nullable(Tuple(a Array(String)))
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS
    serialization_info_version = 'with_subcolumns',
    ratio_of_defaults_for_sparse_serialization = 0.5;

INSERT INTO nullable_tuple_array_sparse
SELECT CAST(tuple([if(number = 0, 'value', '')]), 'Nullable(Tuple(a Array(String)))')
FROM numbers(100);

SELECT dumpColumnStructure(t) LIKE '%Sparse%'
FROM nullable_tuple_array_sparse
LIMIT 1;

SELECT count(), countIf(t.a = ['value']), countIf(t.a = [''])
FROM nullable_tuple_array_sparse;

DROP TABLE nullable_tuple_array_sparse;
