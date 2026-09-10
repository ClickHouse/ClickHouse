-- A Tuple column's flattened sub-column name can coincide with a separate physical column name
-- (e.g. `n Tuple(a)` flattens to `n.a`, which collides with a quoted physical column `n.a`). Such
-- a table is rejected at creation because the two columns' serialization streams collide, so the
-- ambiguous flattened header can never reach the merge path. This holds regardless of
-- allow_tuple_element_aggregation, so a flattened name can never shadow a real physical column.

DROP TABLE IF EXISTS t_leaf_collision_enabled;
DROP TABLE IF EXISTS t_leaf_collision_disabled;
DROP TABLE IF EXISTS t_leaf_collision_nested;

-- Feature enabled: rejected.
CREATE TABLE t_leaf_collision_enabled (k UInt32, n Tuple(a UInt32), `n.a` UInt32)
ENGINE = SummingMergeTree ORDER BY k
SETTINGS allow_tuple_element_aggregation = 1; -- { serverError BAD_ARGUMENTS }

-- Feature disabled (default): same rejection — it is a storage-level check, not feature-gated.
CREATE TABLE t_leaf_collision_disabled (k UInt32, n Tuple(a UInt32), `n.a` UInt32)
ENGINE = SummingMergeTree ORDER BY k; -- { serverError BAD_ARGUMENTS }

-- A deeper leaf colliding with a quoted dotted physical column is rejected too.
CREATE TABLE t_leaf_collision_nested (k UInt32, n Tuple(a Tuple(b UInt32)), `n.a.b` UInt32)
ENGINE = SummingMergeTree ORDER BY k
SETTINGS allow_tuple_element_aggregation = 1; -- { serverError BAD_ARGUMENTS }

-- Two leaves can also collide inside a single Tuple column: a nested element `a.b` and a sibling
-- element literally named `a.b` both flatten to `n.a.b`. When the feature is enabled this is
-- rejected at CREATE so the ambiguous flattened header never reaches a merge.
DROP TABLE IF EXISTS t_intra_tuple_collision;
CREATE TABLE t_intra_tuple_collision (k UInt32, n Tuple(a Tuple(b UInt32), `a.b` UInt32))
ENGINE = SummingMergeTree ORDER BY k
SETTINGS allow_tuple_element_aggregation = 1; -- { serverError BAD_ARGUMENTS }

SELECT 'tuple leaf vs physical column name collision is rejected at CREATE';
