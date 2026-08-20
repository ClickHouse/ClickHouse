-- Under group_by_use_nulls, a projection equal to a constant GROUP BY key is replaced with a
-- Nullable clone of the key (scope.nullable_group_by_keys). Two constants with equal value and
-- type but different source expressions collide there (ConstantNode equality/hash ignore the
-- source expression), so the clone must be taken from the matched node itself, not from a
-- possibly-different colliding key: the source expression determines the planner action node name
-- and thus which aggregation key column the projection reads.
--
-- `a` is toUInt8(1) (UInt8) and `b` is CAST(1, 'Nullable(UInt8)') (Nullable(UInt8)); both have
-- value 1, and the Nullable-converted shape of `a` collides with the original shape of `b`. The
-- projection of `a` must keep `toUInt8` as its source, so `toUInt8` appears as a source expression
-- twice (the projection of `a` and the GROUP BY key `a`). When mis-bound it appears only once.

SET enable_analyzer = 1;
SET group_by_use_nulls = 1;

SELECT countIf(explain LIKE '%function_name: toUInt8%') AS toUInt8_source_count
FROM
(
    EXPLAIN QUERY TREE run_passes = 1
    SELECT toUInt8(1) AS a, CAST(1, 'Nullable(UInt8)') AS b
    GROUP BY a, b WITH CUBE
);
