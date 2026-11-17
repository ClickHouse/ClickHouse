-- When `allow_tuple_element_aggregation` is enabled, the schema constraints enforced at table
-- creation must also be enforced by `ALTER`, otherwise an opt-in table could be altered into a
-- state that CREATE/ATTACH refuse: Nullable Tuple columns (tuple elements are flattened and
-- aggregated independently, which a Nullable Tuple cannot represent) and plain Tuple columns in
-- the sorting key (flattening renames the ordering column's leaves).

SET allow_experimental_nullable_tuple_type = 1;

DROP TABLE IF EXISTS t_alter_nullable_tuple;

CREATE TABLE t_alter_nullable_tuple
(
    id UInt64,
    v UInt64,
    n Tuple(a UInt64)
)
ENGINE = SummingMergeTree
ORDER BY id
SETTINGS allow_tuple_element_aggregation = 1;

-- ADD COLUMN of a Nullable Tuple type is rejected.
ALTER TABLE t_alter_nullable_tuple ADD COLUMN m Nullable(Tuple(b UInt64)); -- { serverError NOT_IMPLEMENTED }

-- MODIFY COLUMN turning an existing plain Tuple into a Nullable Tuple is rejected.
ALTER TABLE t_alter_nullable_tuple MODIFY COLUMN n Nullable(Tuple(a UInt64)); -- { serverError NOT_IMPLEMENTED }

DROP TABLE t_alter_nullable_tuple;

-- A plain Tuple column may not enter the sorting key either: flattening renames its leaves, so
-- the ordering column would no longer exist at merge time. Adding a brand-new Tuple column to
-- the sorting key in a single `ALTER` would otherwise bypass the CREATE-time check.
DROP TABLE IF EXISTS t_alter_tuple_sorting_key;

CREATE TABLE t_alter_tuple_sorting_key
(
    id UInt64,
    v UInt64
)
ENGINE = SummingMergeTree
ORDER BY id
SETTINGS allow_tuple_element_aggregation = 1;

ALTER TABLE t_alter_tuple_sorting_key ADD COLUMN nt Tuple(x UInt64, y UInt64), MODIFY ORDER BY (id, nt); -- { serverError NOT_IMPLEMENTED }

DROP TABLE t_alter_tuple_sorting_key;
