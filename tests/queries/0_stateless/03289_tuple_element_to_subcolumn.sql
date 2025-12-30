DROP TABLE IF EXISTS t_tuple_elem;

SET enable_variant_type = 1;

CREATE TABLE t_tuple_elem (
    t1 Tuple(
        a Array(UInt64),
        b Array(LowCardinality(String))),
    v Variant(
        Array(UInt64),
        Array(LowCardinality(String)))
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_tuple_elem VALUES (([1, 2], ['a', 'b', 'c']), [3, 4]);
INSERT INTO t_tuple_elem VALUES (([3, 4], ['d', 'e']), ['d', 'e']);

SET optimize_functions_to_subcolumns = 1;

SELECT (tupleElement(t1, 1), tupleElement(t1, 2)) FROM t_tuple_elem ORDER BY ALL;
SELECT (tupleElement(t1, 'a'), tupleElement(t1, 'b')) FROM t_tuple_elem ORDER BY ALL;
SELECT (variantElement(v, 'Array(UInt64)'), variantElement(v, 'Array(LowCardinality(String))')) FROM t_tuple_elem ORDER BY ALL;

DROP TABLE t_tuple_elem;
