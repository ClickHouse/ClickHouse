-- Regression test: an explicit tuple-element `DEFAULT NULL` promotes the element type to `Nullable`,
-- and a type that is already nullable - `Nullable(...)` or `LowCardinality(Nullable(...))` - must be
-- kept as is. Wrapping `LowCardinality(Nullable(String))` again produced the invalid type
-- `LowCardinality(Nullable(Nullable(String)))`, which is rejected by `DataTypeNullable`.
-- See https://github.com/ClickHouse/ClickHouse/issues/2797.

DROP TABLE IF EXISTS t_tuple_default_null_lc;

CREATE TABLE t_tuple_default_null_lc
(
    id UInt8,
    already_nullable Tuple(a Nullable(String) DEFAULT NULL),
    already_nullable_lc Tuple(a LowCardinality(Nullable(String)) DEFAULT NULL),
    promoted_lc Tuple(a LowCardinality(String) DEFAULT NULL)
)
ENGINE = MergeTree ORDER BY id;

SELECT name, type, default_kind, default_expression
FROM system.columns
WHERE database = currentDatabase() AND table = 't_tuple_default_null_lc' AND name != 'id'
ORDER BY name;

INSERT INTO t_tuple_default_null_lc (id) VALUES (1);
SELECT already_nullable, already_nullable_lc, promoted_lc FROM t_tuple_default_null_lc;

DROP TABLE t_tuple_default_null_lc;
