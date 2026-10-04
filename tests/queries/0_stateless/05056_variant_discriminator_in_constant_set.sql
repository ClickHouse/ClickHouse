-- A `Variant` value carries which alternative it occupies, and a `Field` does not. Building the set of
-- an `IN` rebuilt every element from a `Field`, so a value stored as `UInt64` was re-selected into the
-- `Date` alternative and stopped matching the row it was built from. 33 of the 52 rows below answer
-- differently without the fix; the last two are controls that must not move. Every `OR` chain is asserted
-- against the same chain un-rewritten as ground truth, and four of them against the plan as well, so a
-- decline cannot pass for a fix.

SET enable_analyzer = 1;

-- A set element keeps the alternative its own type names, so it matches the row holding that alternative
-- (and, on the row below, stops matching a row that holds another alternative with the same value).
SELECT 'in-typed-alternative', count() FROM (SELECT materialize(1::UInt64::Variant(Date, UInt64)) AS v) WHERE v IN (1::UInt64, 5::UInt64, 7::UInt64);
SELECT 'in-typed-alternative-other-row', count() FROM (SELECT materialize(toDate(1)::Variant(Date, UInt64)) AS v) WHERE v IN (1::UInt64);
SELECT 'in-variant-constant', count() FROM (SELECT materialize(1::UInt64::Variant(Date, UInt64)) AS v) WHERE v IN (1::UInt64::Variant(Date, UInt64), 5::UInt64::Variant(Date, UInt64));
SELECT 'in-variant-constant-nested', count() FROM (SELECT materialize([1::UInt64]::Array(Variant(Date, UInt64))) AS a) WHERE a IN ([1::UInt64]::Array(Variant(Date, UInt64)), [5::UInt64]::Array(Variant(Date, UInt64)));

-- `Array`, `Map` and `Tuple` are converted element-wise, so an element type that names an alternative
-- keeps it at any depth, and a row holding another alternative stops matching.
SELECT 'in-nested-typed-alternative', count() FROM (SELECT materialize([1::UInt64]::Array(Variant(Date, UInt64))) AS a) WHERE a IN ([1::UInt64]);
SELECT 'in-nested-typed-alternative-other-row', count() FROM (SELECT materialize([toDate(1)]::Array(Variant(Date, UInt64))) AS a) WHERE a IN ([1::UInt64]);
SELECT 'in-nested-variant-source-extension', count() FROM (SELECT materialize([1::UInt64]::Array(Variant(Date, UInt64))) AS a) WHERE a IN ([1::UInt64::Variant(UInt64)]);
SELECT 'in-map-value-typed-alternative', count() FROM (SELECT materialize(map('k', 1::UInt64)::Map(String, Variant(Date, UInt64))) AS m) WHERE m IN (map('k', 1::UInt64));
SELECT 'in-map-value-typed-alternative-other-row', count() FROM (SELECT materialize(map('k', toDate(1))::Map(String, Variant(Date, UInt64))) AS m) WHERE m IN (map('k', 1::UInt64));
SELECT 'in-tuple-element-typed-alternative', count() FROM (SELECT materialize(tuple(1::UInt64)::Tuple(Variant(Date, UInt64))) AS t) WHERE t IN (tuple(1::UInt64));
SELECT 'in-nested-twice', count() FROM (SELECT materialize([[1::UInt64]]::Array(Array(Variant(Date, UInt64)))) AS a) WHERE a IN ([[1::UInt64]]);
SELECT 'in-nested-nullable-element', count() FROM (SELECT materialize([1::UInt64]::Array(Variant(Date, UInt64))) AS a) WHERE a IN (CAST([1], 'Array(Nullable(UInt64))'));
-- ... while a nested source type that names no alternative is one `CAST` refuses outright, so it stays on
-- the `Field` path instead of becoming a conversion error.
SELECT 'in-nested-no-such-alternative', count() FROM (SELECT materialize([1::UInt64]::Array(Variant(Date, UInt64))) AS a) WHERE a IN ([1::UInt8]);

-- A `Nullable` on the target side wraps a nested conversion `CAST` performs unchanged, so the alternative
-- is chosen by type under one as well. A `Variant` cannot be inside `Nullable`; a composite carrying one
-- can. The `equals` row is the ground truth for the `IN` above it, and the last two rows hold the other
-- alternative and a NULL, so they must not match.
SELECT 'in-nullable-target-under-array', count() FROM (SELECT materialize(CAST([tuple(1::UInt64)], 'Array(Nullable(Tuple(Variant(Date, UInt64))))')) AS a) WHERE a IN (CAST([tuple(1::UInt64)], 'Array(Tuple(UInt64))'));
SELECT 'in-nullable-target-under-tuple', count() FROM (SELECT materialize(CAST(tuple(tuple(1::UInt64)), 'Tuple(Nullable(Tuple(Variant(Date, UInt64))))')) AS t) WHERE t IN (CAST(tuple(tuple(1::UInt64)), 'Tuple(Tuple(UInt64))'));
SELECT 'in-nullable-target-under-tuple-equals', count() FROM (SELECT materialize(CAST(tuple(tuple(1::UInt64)), 'Tuple(Nullable(Tuple(Variant(Date, UInt64))))')) AS t) WHERE t = CAST(tuple(tuple(1::UInt64)), 'Tuple(Tuple(UInt64))');
SELECT 'in-nullable-target-under-map-value', count() FROM (SELECT materialize(CAST(map('k', tuple(1::UInt64)), 'Map(String, Nullable(Tuple(Variant(Date, UInt64))))')) AS m) WHERE m IN (CAST(map('k', tuple(1::UInt64)), 'Map(String, Tuple(UInt64))'));
SELECT 'in-nullable-target-other-row', count() FROM (SELECT materialize(CAST([tuple(toDate(1))], 'Array(Nullable(Tuple(Variant(Date, UInt64))))')) AS a) WHERE a IN (CAST([tuple(1::UInt64)], 'Array(Tuple(UInt64))'));
SELECT 'in-nullable-target-null-element', count() FROM (SELECT materialize(CAST([NULL], 'Array(Nullable(Tuple(Variant(Date, UInt64))))')) AS a) WHERE a IN (CAST([tuple(1::UInt64)], 'Array(Tuple(UInt64))'));
-- ... while a `Nullable` on the SOURCE side is followed at the outer level whenever the constant's row is
-- not NULL, since `CAST` then has no NULL to place, and below that level only where the target holds a NULL
-- of its own. The `equals` row is again the ground truth, and the last row carries a nested NULL the target
-- accepts only through a `Variant` discriminator.
SELECT 'in-nonnull-nullable-source', count() FROM (SELECT materialize(CAST(tuple(1::UInt64), 'Tuple(Variant(Date, UInt64))')) AS t) WHERE t IN (CAST(tuple(1::UInt64), 'Nullable(Tuple(UInt64))'));
SELECT 'in-nonnull-nullable-source-equals', count() FROM (SELECT materialize(CAST(tuple(1::UInt64), 'Tuple(Variant(Date, UInt64))')) AS t) WHERE t = CAST(tuple(1::UInt64), 'Nullable(Tuple(UInt64))');
SELECT 'in-nonnull-nullable-source-nested-array', count() FROM (SELECT materialize(CAST(tuple([1::UInt64]), 'Tuple(Array(Variant(Date, UInt64)))')) AS t) WHERE t IN (CAST(tuple([1::UInt64]), 'Nullable(Tuple(Array(UInt64)))'));
SELECT 'in-nonnull-nullable-source-other-row', count() FROM (SELECT materialize(CAST(tuple(toDate(1)), 'Tuple(Variant(Date, UInt64))')) AS t) WHERE t IN (CAST(tuple(1::UInt64), 'Nullable(Tuple(UInt64))'));
SELECT 'in-nonnull-nullable-source-null-element', count() FROM (SELECT materialize(CAST(tuple(1::UInt64), 'Tuple(Variant(Date, UInt64))')) AS t) WHERE t IN (CAST(tuple(NULL), 'Nullable(Tuple(Nullable(UInt64)))'));
-- ... and a NULL row against a target that holds none keeps the `Field` path, which answers "not
-- representable" and has the set skip it rather than failing the query.
SELECT 'in-null-constant-nonnullable-target', count() FROM (SELECT materialize(CAST(tuple(1::UInt64), 'Tuple(Variant(Date, UInt64))')) AS t) WHERE t IN (CAST(NULL, 'Nullable(Tuple(UInt64))'));
SELECT 'in-null-constant-nonnullable-target-nested', count() FROM (SELECT materialize(CAST([tuple(1::UInt64)], 'Array(Tuple(Variant(Date, UInt64)))')) AS a) WHERE a IN (CAST([NULL], 'Array(Nullable(Tuple(UInt64)))'));

-- The `OR` chain the optimizer turns into such an `IN`, with a `Variant` nested in an `Array`.
SELECT 'or-chain-nested', count() FROM (SELECT materialize([1::UInt64]::Array(Variant(Date, UInt64))) AS a)
WHERE a = [1::UInt64]::Array(Variant(Date, UInt64)) OR a = [5::UInt64]::Array(Variant(Date, UInt64)) OR a = [7::UInt64]::Array(Variant(Date, UInt64));
SELECT 'or-chain-nested-unrewritten', count() FROM (SELECT materialize([1::UInt64]::Array(Variant(Date, UInt64))) AS a)
WHERE a = [1::UInt64]::Array(Variant(Date, UInt64)) OR a = [5::UInt64]::Array(Variant(Date, UInt64)) OR a = [7::UInt64]::Array(Variant(Date, UInt64))
SETTINGS optimize_min_equality_disjunction_chain_length = 100;
-- ... and the rewrite is declined: a set element keys on one discriminator, while the `equals` it would
-- replace is evaluated against each row's active alternative, so the two are different relations.
SELECT 'or-chain-nested-is-declined', count() FROM (EXPLAIN QUERY TREE
    SELECT count() FROM (SELECT materialize([1::UInt64]::Array(Variant(Date, UInt64))) AS a)
    WHERE a = [1::UInt64]::Array(Variant(Date, UInt64)) OR a = [5::UInt64]::Array(Variant(Date, UInt64)) OR a = [7::UInt64]::Array(Variant(Date, UInt64)))
WHERE explain ILIKE '%function_name: in%';

-- The same for a top-level `Variant` expression.
SELECT 'or-chain-top-level', count() FROM (SELECT materialize(1::UInt64::Variant(Date, UInt64)) AS v)
WHERE v = 1::UInt64::Variant(Date, UInt64) OR v = 5::UInt64::Variant(Date, UInt64) OR v = 7::UInt64::Variant(Date, UInt64);
SELECT 'or-chain-top-level-unrewritten', count() FROM (SELECT materialize(1::UInt64::Variant(Date, UInt64)) AS v)
WHERE v = 1::UInt64::Variant(Date, UInt64) OR v = 5::UInt64::Variant(Date, UInt64) OR v = 7::UInt64::Variant(Date, UInt64)
SETTINGS optimize_min_equality_disjunction_chain_length = 100;
SELECT 'or-chain-top-level-is-declined', count() FROM (EXPLAIN QUERY TREE
    SELECT count() FROM (SELECT materialize(1::UInt64::Variant(Date, UInt64)) AS v)
    WHERE v = 1::UInt64::Variant(Date, UInt64) OR v = 5::UInt64::Variant(Date, UInt64) OR v = 7::UInt64::Variant(Date, UInt64))
WHERE explain ILIKE '%function_name: in%';

-- The `notEquals` seam, whose first conjunct is false, so no row may survive.
SELECT 'not-in-nested', count() FROM (SELECT materialize([1::UInt64]::Array(Variant(Date, UInt64))) AS a, materialize(toUInt8(1)) AS x)
WHERE a != [1::UInt64]::Array(Variant(Date, UInt64)) AND a != [5::UInt64]::Array(Variant(Date, UInt64)) AND a != [7::UInt64]::Array(Variant(Date, UInt64)) AND x = 1;
-- ... and that chain is declined too, so the row above is the un-rewritten chain's own answer and not a
-- `notIn` that happens to agree with it.
SELECT 'not-in-nested-is-declined', count() FROM (EXPLAIN QUERY TREE
    SELECT count() FROM (SELECT materialize([1::UInt64]::Array(Variant(Date, UInt64))) AS a, materialize(toUInt8(1)) AS x)
    WHERE a != [1::UInt64]::Array(Variant(Date, UInt64)) AND a != [5::UInt64]::Array(Variant(Date, UInt64)) AND a != [7::UInt64]::Array(Variant(Date, UInt64)) AND x = 1)
WHERE explain ILIKE '%function_name: notIn%';

-- A constant whose type is not one of the alternatives has no faithful place in the set: `equals` is
-- evaluated against each row's active alternative (it even throws when that alternative is not
-- comparable with the constant), while a set element is fixed. So the chain must be kept instead.
SELECT 'or-chain-untyped-constants', count() FROM (SELECT materialize(1::UInt64::Variant(Date, UInt64)) AS v) WHERE v = 1 OR v = 5 OR v = 7;
SELECT 'or-chain-untyped-constants-unrewritten', count() FROM (SELECT materialize(1::UInt64::Variant(Date, UInt64)) AS v) WHERE v = 1 OR v = 5 OR v = 7
SETTINGS optimize_min_equality_disjunction_chain_length = 100;
SELECT 'or-chain-untyped-constants-is-declined', count() FROM (EXPLAIN QUERY TREE
    SELECT count() FROM (SELECT materialize(1::UInt64::Variant(Date, UInt64)) AS v) WHERE v = 1 OR v = 5 OR v = 7)
WHERE explain ILIKE '%function_name: in%';

-- `equals` can match a numerically equal value under ANOTHER alternative, which no single set element
-- reproduces, so a constant that does name an alternative is not enough to make the rewrite equivalent.
SELECT 'or-chain-cross-alternative', count() FROM (SELECT materialize(42::UInt8::Variant(UInt8, UInt64)) AS v)
WHERE v = 42::UInt64 OR v = 5::UInt64 OR v = 7::UInt64 SETTINGS allow_suspicious_variant_types = 1;
SELECT 'or-chain-cross-alternative-unrewritten', count() FROM (SELECT materialize(42::UInt8::Variant(UInt8, UInt64)) AS v)
WHERE v = 42::UInt64 OR v = 5::UInt64 OR v = 7::UInt64
SETTINGS allow_suspicious_variant_types = 1, optimize_min_equality_disjunction_chain_length = 100;

-- Comparison pruning reasons about the constants as `Field`s too, so two of them under different
-- alternatives collided as map keys and one of two mutually exclusive conditions was dropped.
SELECT 'pruning-different-alternatives', count() FROM
    (SELECT materialize(toDate(1)::Variant(Date, UInt64)) AS v UNION ALL SELECT materialize(1::UInt64::Variant(Date, UInt64)) AS v)
WHERE v != toDate(1)::Variant(Date, UInt64) AND v != 1::UInt64::Variant(Date, UInt64)
SETTINGS use_variant_default_implementation_for_comparisons = 0;
SELECT 'pruning-different-alternatives-off', count() FROM
    (SELECT materialize(toDate(1)::Variant(Date, UInt64)) AS v UNION ALL SELECT materialize(1::UInt64::Variant(Date, UInt64)) AS v)
WHERE v != toDate(1)::Variant(Date, UInt64) AND v != 1::UInt64::Variant(Date, UInt64)
SETTINGS use_variant_default_implementation_for_comparisons = 0, optimize_redundant_comparisons = 0;

-- Types that `IDataType::equals` treats as equal are not enough either: it ignores a `DateTime`
-- timezone, while the alternative is looked up by the type name the constant spells.
SELECT 'or-chain-equal-but-differently-named', count() FROM (SELECT materialize([1::UInt64]::Variant(Array(DateTime('UTC')), Array(UInt64))) AS v)
WHERE v = [1::UInt64]::Variant(Array(DateTime('Asia/Tokyo')), Array(UInt64)) OR v = [5::UInt64]::Variant(Array(DateTime('Asia/Tokyo')), Array(UInt64)) OR v = [7::UInt64]::Variant(Array(DateTime('Asia/Tokyo')), Array(UInt64));
SELECT 'or-chain-equal-but-differently-named-unrewritten', count() FROM (SELECT materialize([1::UInt64]::Variant(Array(DateTime('UTC')), Array(UInt64))) AS v)
WHERE v = [1::UInt64]::Variant(Array(DateTime('Asia/Tokyo')), Array(UInt64)) OR v = [5::UInt64]::Variant(Array(DateTime('Asia/Tokyo')), Array(UInt64)) OR v = [7::UInt64]::Variant(Array(DateTime('Asia/Tokyo')), Array(UInt64))
SETTINGS optimize_min_equality_disjunction_chain_length = 100;

-- A `Variant` source keeps its alternative when the target merely extends it.
SELECT 'variant-source-extension-in', count() FROM (SELECT materialize(1::UInt64::Variant(Date, UInt64)) AS v) WHERE v IN (1::UInt64::Variant(UInt64));
SELECT 'variant-source-extension-eq', count() FROM (SELECT materialize(1::UInt64::Variant(Date, UInt64)) AS v) WHERE v = 1::UInt64::Variant(UInt64);

-- Only what `CAST` can place by type goes down the column path, and it keeps an ordinary
-- `LowCardinality` (which can be an alternative itself), so such a constant stays on the `Field` path
-- rather than becoming a conversion error. A `Nullable` wrapper is stripped the same way `CAST` strips it.
SELECT 'lc-constant-or-chain', count() FROM (SELECT materialize('a'::Variant(String, UInt64)) AS v)
WHERE v = 'a'::LowCardinality(String) OR v = 'b'::LowCardinality(String) OR v = 'c'::LowCardinality(String);
SELECT 'lc-constant-or-chain-unrewritten', count() FROM (SELECT materialize('a'::Variant(String, UInt64)) AS v)
WHERE v = 'a'::LowCardinality(String) OR v = 'b'::LowCardinality(String) OR v = 'c'::LowCardinality(String)
SETTINGS optimize_min_equality_disjunction_chain_length = 100;
SELECT 'lc-constant-values', count() FROM VALUES('v Variant(String, UInt64)', ('a'::LowCardinality(String)), ('b'::LowCardinality(String)));
-- ... and that chain is declined too, so the row above is the chain's own answer:
SELECT 'lc-constant-or-chain-is-declined', count() FROM (EXPLAIN QUERY TREE
    SELECT count() FROM (SELECT materialize('a'::Variant(String, UInt64)) AS v)
    WHERE v = 'a'::LowCardinality(String) OR v = 'b'::LowCardinality(String) OR v = 'c'::LowCardinality(String))
WHERE explain ILIKE '%function_name: in%';
SELECT 'nullable-constant-in', count() FROM (SELECT materialize(1::UInt64::Variant(Date, UInt64)) AS v) WHERE v IN (1::Nullable(UInt64));

-- Nothing outside a `Variant` changes: an identity conversion still goes through the `Field` path, which
-- clamps a raw byte a column can hold outside its type's domain.
SELECT 'bool-raw-byte-still-clamped', reinterpret(toUInt8(2), 'Bool') = reinterpret(toUInt8(2), 'Bool'), reinterpret(toUInt8(2), 'Bool') IN (reinterpret(toUInt8(2), 'Bool'), false);
-- ... and the rewrite keeps firing for a plain type.
SELECT 'plain-type-still-rewritten', count() FROM (EXPLAIN QUERY TREE
    SELECT count() FROM (SELECT materialize(1::UInt64) AS u) WHERE u = 1 OR u = 5 OR u = 7)
WHERE explain ILIKE '%function_name: in%';
