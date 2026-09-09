-- A folded constant of `Dynamic` type must reach a secondary server carrying the type its active
-- member actually has, not the type its bare literal happens to infer back to.

-- The path is analyzer-only. `prefer_localhost_replica` = 0 forces the serialized remote plan
-- instead of a local one, `serialize_query_plan` = 0 keeps the constant in the query text rather
-- than in a serialized plan, and `enable_parallel_replicas` = 0 pins the runner's randomization
-- (a query-level SET beats it) so every cell reads the same plan shape.
SET enable_analyzer = 1;
SET prefer_localhost_replica = 0;
SET serialize_query_plan = 0;
SET enable_parallel_replicas = 0;

-- An `Enum8` member: its literal is the underlying number, which infers back as `UInt8`.
-- `materialize` is required. Without it the initiator folds `dynamicType` itself and ships its
-- own answer as a string literal, so the cell would pass whatever the shard rebuilt.
SELECT DISTINCT dynamicType(materialize(CAST(CAST('7', 'Enum8(\'7\' = 3)') AS Dynamic)))
FROM remote('127.0.0.1', system.one);

-- The user-visible failure: comparing against the rebuilt member type raises `NO_COMMON_TYPE`
-- when it arrives as `UInt8`, because {`String`, `UInt8`} has no supertype while {`String`, `Enum8`} does.
DROP TABLE IF EXISTS t_dynamic_const_fold;
CREATE TABLE t_dynamic_const_fold (v String) ENGINE = MergeTree ORDER BY v;
INSERT INTO t_dynamic_const_fold VALUES ('7'), ('3');

SELECT v FROM remote('127.0.0.1', currentDatabase(), t_dynamic_const_fold)
WHERE v = CAST(CAST('7', 'Enum8(\'7\' = 3)') AS Dynamic);

-- `Dynamic(max_types = 0)` keeps every value in the shared binary variant, which is decoded by a
-- second code path.
SELECT DISTINCT dynamicType(materialize(CAST(CAST('7', 'Enum8(\'7\' = 3)') AS Dynamic(max_types = 0))))
FROM remote('127.0.0.1', system.one);

-- `array` resolves one element type from all its arguments. Elements named as their own concrete
-- types need `use_variant_as_common_type` to acquire a common type at all, while unnamed literals of
-- different widths simply widen, so a `Dynamic` inside an `array` is left exactly as it was.
SELECT DISTINCT arrayMap(x -> dynamicType(x), materialize([1::Int64::Dynamic, 1::UInt64::Dynamic]))
FROM remote('127.0.0.1', system.one) SETTINGS use_variant_as_common_type = 0;

-- `map` resolves one key type and one value type from all of its arguments, so the rule applies to
-- both halves of a `Dynamic`-keyed, `Dynamic`-valued `map`. Two entries are needed: with one entry there
-- are no siblings to reconcile and the restriction is invisible.
SELECT DISTINCT arrayMap(x -> dynamicType(x), mapKeys(m)), arrayMap(x -> dynamicType(x), mapValues(m))
FROM (
    SELECT materialize(map(1::Int64::Dynamic, 3::Int64::Dynamic, 2::UInt64::Dynamic, 4::UInt64::Dynamic)) AS m
    FROM remote('127.0.0.1', system.one))
SETTINGS use_variant_as_common_type = 0;

-- `tuple` reconciles nothing between its arguments, it keeps each one's type, so a `Dynamic` inside
-- a `tuple` IS named: under the very setting that makes the two cells above give up, an `Int64` and
-- a `UInt64` member coexist. An `IN` list is serialized as a `tuple`, so this is the shape a
-- `Dynamic` constant in a list travels in.
SELECT DISTINCT dynamicType(tupleElement(t, 1)), dynamicType(tupleElement(t, 2))
FROM (
    SELECT materialize(tuple(1::Int64::Dynamic, 2::UInt64::Dynamic)) AS t
    FROM remote('127.0.0.1', system.one))
SETTINGS use_variant_as_common_type = 0;

-- The shared binary variant is a second value exit and it forwards the same restriction: a
-- `Dynamic(max_types = 0)` inside an `array` must stay unnamed too, or its two elements arrive named
-- as `Int64` and `UInt64` and `array` has no common type for them.
SELECT DISTINCT arrayMap(x -> dynamicType(x), materialize(
    [1::Int64::Dynamic(max_types = 0), 2::UInt64::Dynamic(max_types = 0)]))
FROM remote('127.0.0.1', system.one) SETTINGS use_variant_as_common_type = 0;

-- A `DateTime` is written as local text, and both instants of a DST overlap share that text, so
-- naming its member type would turn a visible type mismatch into a silently different instant.
-- 1698543000 is the later of the two occurrences; its own text re-parses to the earlier one.
SELECT DISTINCT dynamicType(c), c FROM (
    SELECT materialize(CAST(toDateTime(1698543000, 'Europe/Berlin') AS Dynamic)) AS c
    FROM remote('127.0.0.1', system.one))
SETTINGS cast_string_to_dynamic_use_inference = 0;

-- A string-like member is left unnamed: the receiving side decides with
-- `cast_string_to_dynamic_use_inference` whether to re-parse the text, so a named `FixedString(3)` would
-- arrive as `FixedString(3)` with the setting off and as the inferred type with it on. Left unnamed it is a
-- `String` either way. `String` itself cannot show this, since naming it changes nothing, so the cell uses
-- `FixedString(3)` with a value that fills the type exactly, so no padding reaches the reference.
SELECT DISTINCT dynamicType(c), c FROM (
    SELECT materialize(CAST(CAST('abc', 'FixedString(3)') AS Dynamic)) AS c
    FROM remote('127.0.0.1', system.one))
SETTINGS cast_string_to_dynamic_use_inference = 0;

-- A `Dynamic` whose active member is a `tuple` of `Dynamic`s is only partly left alone: the tuple's
-- members are named one level down while the outer member type declines, so the shard rebuilds
-- `Tuple(Int64, UInt64)` where master rebuilt `Tuple(UInt8, UInt8)` and the initiator held
-- `Tuple(Dynamic, Dynamic)`. The cell records that state; it is not an invariant to preserve.
SELECT DISTINCT dynamicType(c) FROM (
    SELECT materialize(CAST(tuple(1::Int64::Dynamic, 2::UInt64::Dynamic) AS Dynamic)) AS c
    FROM remote('127.0.0.1', system.one));

DROP TABLE t_dynamic_const_fold;
