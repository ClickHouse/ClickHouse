-- A folded constant of Dynamic type must reach a secondary server carrying the type its active
-- member actually has, not the type its bare literal happens to infer back to.

-- The path is analyzer-only. prefer_localhost_replica = 0 forces the serialized remote plan
-- instead of a local one, serialize_query_plan = 0 keeps the constant in the query text rather
-- than in a serialized plan, and enable_parallel_replicas = 0 pins the runner's randomization
-- (a query-level SET beats it) so every cell reads the same plan shape.
SET enable_analyzer = 1;
SET prefer_localhost_replica = 0;
SET serialize_query_plan = 0;
SET enable_parallel_replicas = 0;

-- An Enum8 member: its literal is the underlying number, which infers back as UInt8.
-- materialize() is required. Without it the initiator folds dynamicType() itself and ships its
-- own answer as a string literal, so the cell would pass whatever the shard rebuilt.
SELECT DISTINCT dynamicType(materialize(CAST(CAST('7', 'Enum8(\'7\' = 3)') AS Dynamic)))
FROM remote('127.0.0.1', system.one);

-- The user-visible failure: comparing against the rebuilt member type raises NO_COMMON_TYPE
-- when it arrives as UInt8, because {String, UInt8} has no supertype while {String, Enum8} does.
DROP TABLE IF EXISTS t_dynamic_const_fold;
CREATE TABLE t_dynamic_const_fold (v String) ENGINE = MergeTree ORDER BY v;
INSERT INTO t_dynamic_const_fold VALUES ('7'), ('3');

SELECT v FROM remote('127.0.0.1', currentDatabase(), t_dynamic_const_fold)
WHERE v = CAST(CAST('7', 'Enum8(\'7\' = 3)') AS Dynamic);

-- Dynamic(max_types = 0) keeps every value in the shared binary variant, which is decoded by a
-- second code path.
SELECT DISTINCT dynamicType(materialize(CAST(CAST('7', 'Enum8(\'7\' = 3)') AS Dynamic(max_types = 0))))
FROM remote('127.0.0.1', system.one);

-- array() resolves one element type from all its arguments. Elements named as their own concrete
-- types need use_variant_as_common_type to acquire a common type at all, while unnamed literals of
-- different widths simply widen, so a Dynamic inside an array is left exactly as it was.
SELECT DISTINCT arrayMap(x -> dynamicType(x), materialize([1::Int64::Dynamic, 1::UInt64::Dynamic]))
FROM remote('127.0.0.1', system.one) SETTINGS use_variant_as_common_type = 0;

-- A DateTime is written as local text, and both instants of a DST overlap share that text, so
-- naming its member type would turn a visible type mismatch into a silently different instant.
-- 1698543000 is the later of the two occurrences; its own text re-parses to the earlier one.
SELECT DISTINCT dynamicType(c), c FROM (
    SELECT materialize(CAST(toDateTime(1698543000, 'Europe/Berlin') AS Dynamic)) AS c
    FROM remote('127.0.0.1', system.one))
SETTINGS cast_string_to_dynamic_use_inference = 0;

DROP TABLE t_dynamic_const_fold;
