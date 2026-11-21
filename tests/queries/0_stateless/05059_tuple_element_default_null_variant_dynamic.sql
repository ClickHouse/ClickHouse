-- Regression test: a tuple-element `DEFAULT NULL` must not wrap a type that carries `NULL` among its
-- own values and cannot live inside `Nullable` - `Variant(...)` and `Dynamic`. Wrapping them produced
-- the invalid types `Nullable(Variant(...))` and `Nullable(Dynamic)`, rejected by `DataTypeNullable`.
-- See https://github.com/ClickHouse/ClickHouse/issues/2797.

DROP TABLE IF EXISTS t_tuple_default_null_variant_dynamic;

CREATE TABLE t_tuple_default_null_variant_dynamic
(
    id UInt8,
    variant_element Tuple(a Variant(UInt64, String) DEFAULT NULL),
    dynamic_element Tuple(a Dynamic DEFAULT NULL)
)
ENGINE = MergeTree ORDER BY id;

SELECT name, type, default_kind, default_expression
FROM system.columns
WHERE database = currentDatabase() AND table = 't_tuple_default_null_variant_dynamic' AND name != 'id'
ORDER BY name;

INSERT INTO t_tuple_default_null_variant_dynamic (id) VALUES (1);
SELECT variant_element, dynamic_element FROM t_tuple_default_null_variant_dynamic;

DROP TABLE t_tuple_default_null_variant_dynamic;
